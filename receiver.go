package gosqstask

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// Receiver is a struct to receive messages from SQS and schedule them for processing
type Receiver struct {
	// Client is the SQS client.
	// This member is required.
	Client *sqs.Client

	// ReceiveMessageInput is the input for the ReceiveMessage operation.
	// This member is required.
	ReceiveMessageInput *sqs.ReceiveMessageInput

	// Concurrency is the number of concurrent workers to process messages.
	// This is different from the number of messages received from SQS and this number
	// represents pool size of concurrently running task goroutines. For example, if you
	// have ReceiveMessageInput.MaxNumberOfMessages set to 3 and Concurrency member set to 2,
	// then you can receive 3 messages (if available) from SQS in one call and first two
	// messages will be added to the pool to be processed concurrently, while third message
	// will be waiting for at least one of the first two to be finished. Meanwhile, next 3
	// messages will be read from SQS and will be waiting for pool to get free slots.
	// Good start point (if you have multiple worker instances running in the cluster) will
	// be to set ReceiveMessageInput.MaxNumberOfMessages = 1 and Concurrency higher than 1,
	// depending on the capacity of your worker instance.
	//
	// If Concurrency member is not set, default concurrency 1 will be used.
	Concurrency int

	// MessageConfig is the per-message configuration for the Processor
	// Each received message will be passed to this function first to get the
	// per-message configuration.
	// For informational purposes - there is a queueVisTimeout parameter that is
	// passed to the Processor function, which contains SQS queue's default
	// Visibility Timeout value.
	//
	// If this member is not set, default configuration will be used:
	//		AllowLongRunningTasks=false
	MessageConfig func(ctx context.Context, msg *types.Message, queueVisTimeout int) MessageConfig

	// Processor is the function to process the received message. It should return
	// an error if the message processing failed, and you want for the message to be
	// visible again in the queue to give it a chance to be processed again (e.g. if
	// there was a temporary issue with the message processing, such as network error
	// while calling to external service or database etc). If the message processing
	// was successful or even if it failed, but you don't want for it to go back to
	// queue, then you should return nil.
	// Remember, that if message is panicked, it will crash the entire Receiver.Listen
	// function, and it many cases it is undesired, so make sure to handle panics inside
	// the Processor function.
	//
	// This member is required.
	Processor func(ctx context.Context, msg *types.Message, cfg MessageConfig) error

	// Logger
	// This member is optional and if not provided - default StdOut WARN-level logger
	// will be created.
	Logger *slog.Logger

	recvCancelFn context.CancelFunc
	activeTasks  atomic.Int32
}

type ProcessRequest int

const (
	// ProcessRequestDefault is the default status that instructs the Receiver to process the message
	// normally, withing the queue's default Visibility Timeout
	ProcessRequestDefault ProcessRequest = iota

	// ProcessRequestLongRunning is a status to allow long-running tasks, task can potentially take longer than
	//	queue's Visibility Timeout. In this case Receiver will create a new background tracking task that will
	//	be measuring the time since the SQS message was received and if it exceeds the Visibility Timeout,
	//	then message's Visibility Timeout will be extended to remain invisible to other consumers and
	//	in case of failure, the message will be visible again after new Visibility Timeout.
	ProcessRequestLongRunning

	// ProcessRequestSkip is a status to skip the message processing for current consumer. This message
	// will be returned back to queue
	ProcessRequestSkip

	// ProcessRequestDelete is a status to delete the message from the queue without processing it
	ProcessRequestDelete
)

// MessageConfig is the configuration for the Processor
type MessageConfig struct {
	// ProcessRequest is the status to instruct the Receiver how to process the message.
	// Refer to ProcessRequest constants for more information
	// By default ProcessRequestNormal is used
	ProcessRequest ProcessRequest

	// ExpectedProcessingTime is the expected time in seconds that the Processor will take to process the
	// message. This is useful if you use a single queue for multiple types of messages, and some messages
	// require more time (usually much more than your queue's default Visibility Timeout setting) to process
	// than others. It will allow background tracking task to extend message's Visibility Timeout by the value
	// specified in this member, reducing number of ChangeVisibilityTimeout API calls to AWS.
	// This member is optional and if not set, then the default value of queue's visibility timeout
	// will be used.
	ExpectedProcessingTime int

	// User-specific data. This member is optional and can be used to store any user-specific data.
	// As an example - you receive a message from SQS and in order to estimate the processing time
	// you need to unmarshal a payload from the message body to inspect some fields to make a decision
	// about the processing time. You can store this unmarshalled data in UserData member and then
	// use it in the Processor function to avoid deserializing payload again
	UserData any

	// Per-message context. This member is optional and if not set, then the Context passed to
	// Receiver.Listen will be used.
	Context func(ctx context.Context) context.Context

	// Per-message logger. This member is optional and if not set, then the Receiver's logger will be used.
	// This member is useful if you want to log some specific information for this message only,
	// for example - message body, message ID etc.
	Logger func(ctx context.Context, log *slog.Logger, msg *types.Message, cfg MessageConfig) *slog.Logger
}

// Listen starts listening to the queue and processing the messages
func (r *Receiver) Listen(ctx context.Context) error {
	if r.Client == nil {
		return errors.New("member Receiver.Client is required")
	}
	if r.ReceiveMessageInput == nil {
		return errors.New("member Receiver.ReceiveMessageInput is required")
	}
	if r.Processor == nil {
		return errors.New("member Receiver.Processor is required")
	}

	if r.Logger == nil {
		r.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	qattrs, err := r.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: r.ReceiveMessageInput.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameVisibilityTimeout,
		},
	})
	if err != nil {
		return fmt.Errorf("[gosqstask] failed to get queue attributues: %w", err)
	}
	queueVisTimeout, err := strconv.Atoi(qattrs.Attributes[string(types.QueueAttributeNameVisibilityTimeout)])
	if err != nil {
		return fmt.Errorf("[gosqstask] failed to parse queue visibility timeout: %w", err)
	}

	sync := newConcurrencyChannel(r.Concurrency)
	recvCtx, recvCancel := context.WithCancel(ctx)
	r.recvCancelFn = recvCancel

	for {
		result, err := r.Client.ReceiveMessage(recvCtx, r.ReceiveMessageInput)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				r.Logger.InfoContext(ctx, fmt.Sprintf("[gosqstask] canceled listening queue: %v", err))
				break
			}
			r.Logger.ErrorContext(ctx, fmt.Sprintf("[gosqstask] failed to receive message from queue: %v", err))
			continue
		}
		if len(result.Messages) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for i := range result.Messages {
			msg := &result.Messages[i]
			var msgCfg MessageConfig
			if r.MessageConfig != nil {
				msgCfg = r.MessageConfig(ctx, msg, queueVisTimeout)
			}
			if msgCfg.ExpectedProcessingTime == 0 {
				msgCfg.ExpectedProcessingTime = queueVisTimeout
			}

			var msgCtx context.Context
			if msgCfg.Context != nil {
				msgCtx = msgCfg.Context(ctx)
			} else {
				msgCtx = ctx
			}

			var logger *slog.Logger
			if msgCfg.Logger != nil {
				logger = msgCfg.Logger(msgCtx, r.Logger, msg, msgCfg)
			} else {
				logger = r.Logger
			}

			if msgCfg.ProcessRequest == ProcessRequestSkip {
				continue
			}
			if msgCfg.ProcessRequest == ProcessRequestDelete {
				_, err := r.Client.DeleteMessage(msgCtx, &sqs.DeleteMessageInput{
					QueueUrl:      r.ReceiveMessageInput.QueueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})
				if err != nil {
					logger.ErrorContext(msgCtx,
						fmt.Sprintf("[gosqstask] failed to delete message from queue according "+
							"to ProcessRequest command: %v", err))
				}
				continue
			}

			if msgCfg.ExpectedProcessingTime != queueVisTimeout {
				if err = r.changeMsgVisibility(msgCtx, msg.ReceiptHandle, msgCfg.ExpectedProcessingTime); err != nil {
					logger.ErrorContext(msgCtx,
						fmt.Sprintf("[gosqstask] failed to reset message visibility timeout "+""+
							"(error is ignored): %v", err))
				}
			}

			beforeWait := time.Now()
			<-sync
			afterWait := time.Now()
			logger.DebugContext(msgCtx, "[gosqstask] message is getting ready to be processed")
			if afterWait.Sub(beforeWait) > time.Second*1 {
				logger.DebugContext(msgCtx, "[gosqstask] update message visibility after pool wait")
				// It is possible that "<-sync" blocked message processing for some time,
				// and it is better to extend its visibility timeout before processing
				if err = r.changeMsgVisibility(msgCtx, msg.ReceiptHandle, msgCfg.ExpectedProcessingTime); err != nil {
					if !r.isMessageExpiredError(msgCtx, logger, err, "after pool wait") {
						logger.WarnContext(msgCtx, fmt.Sprintf(
							"[gosqstask] failed to reset message visibility timeout after pool wait "+
								"(error is not critical and ignored, message will be processed next time): %v",
							err))
					}
					sync <- struct{}{}
					continue
				}
			}

			// Launch a new goroutine to process the message
			go func(ctx context.Context, logger *slog.Logger, msg *types.Message, msgCfg MessageConfig) {
				logger.DebugContext(ctx, "[gosqstask] message is being processed")
				defer func() {
					r.activeTasks.Add(-1)
					sync <- struct{}{}
				}()
				r.activeTasks.Add(1)
				r.processMessage(ctx, logger, msg, msgCfg)
			}(msgCtx, logger, msg, msgCfg)
		}
	}
	return nil
}

// Shutdown will gracefully shutdown the receiver. First, it will stop receiving new messages
// from SQS, and then it will wait up to "maxWait" duration for all currently processing messages to
// be finished.
func (r *Receiver) Shutdown(ctx context.Context) {
	if r.recvCancelFn != nil {
		r.recvCancelFn()
	}
	for {
		select {
		case <-time.After(1 * time.Second):
			if r.activeTasks.Load() == 0 {
				r.Logger.InfoContext(ctx, "Shutdown initiated because no active tasks left")
				return
			}
		case <-ctx.Done():
			r.Logger.InfoContext(ctx, fmt.Sprintf("Shutdown initiated because context is done: %v", ctx.Err()))
			return
		}
	}
}

func (r *Receiver) processMessage(
	ctx context.Context, logger *slog.Logger, msg *types.Message, cfg MessageConfig) {
	ctx, cancel := context.WithCancel(context.WithValue(ctx, "messageId", *msg.MessageId))

	// If AllowLongRunningTasks is set to true, then create a new background tracking task
	if cfg.ProcessRequest == ProcessRequestLongRunning {
		go func(ctx context.Context) {
			r.longRunningTaskTracker(ctx, logger, msg.ReceiptHandle, cfg.ExpectedProcessingTime)
		}(ctx)
	}

	err := r.Processor(ctx, msg, cfg)
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("[gosqstask] failed to process message: %v", err))
	} else {
		_, err := r.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      r.ReceiveMessageInput.QueueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		})
		if err != nil {
			logger.ErrorContext(ctx, fmt.Sprintf("[gosqstask] failed to delete message from queue: %v", err))
		} else {
			logger.DebugContext(ctx, "[gosqstask] message processed successfully")
		}
	}
	cancel()
}

func (r *Receiver) changeMsgVisibility(ctx context.Context, msgReceipt *string, visTimeout int) error {
	_, err := r.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          r.ReceiveMessageInput.QueueUrl,
		ReceiptHandle:     msgReceipt,
		VisibilityTimeout: int32(visTimeout),
	})
	return err
}

func (r *Receiver) longRunningTaskTracker(
	ctx context.Context, logger *slog.Logger, msgReceipt *string, visTimeout int,
) {
	// Start a ticker to extend message visibility timeout in case if after 3/4 of the original
	// visibility timeout the processing is still not finished
	borderlineTimeout := visTimeout * 3 / 4
	t := time.NewTicker(time.Second * time.Duration(borderlineTimeout))
	defer t.Stop()
	logger.DebugContext(ctx, "[gosqstask] task tracker is started")
	for {
		select {
		case <-ctx.Done():
			logger.DebugContext(ctx, "[gosqstask] shut down task tracker")
			return
		case <-t.C:
			if ctx.Err() != nil {
				logger.DebugContext(ctx, "[gosqstask] ignored changing message visibility timeout")
				return
			}
			logger.DebugContext(ctx, fmt.Sprintf(
				"[gosqstask] extending message visibility timeout for %d seconds", borderlineTimeout))

			err := r.changeMsgVisibility(ctx, msgReceipt, borderlineTimeout)
			if err != nil {
				if !r.isMessageExpiredError(ctx, logger, err, "in task tracker") {
					logger.ErrorContext(ctx, fmt.Sprintf(
						"[gosqstask] failed to extend message visibility timeout in task tracker: %v", err))
				}
			}
		}
	}
}

func (r *Receiver) isMessageExpiredError(ctx context.Context, logger *slog.Logger, err error, op string) bool {
	var apiErr *smithy.GenericAPIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidParameterValue" {
		logger.WarnContext(ctx,
			fmt.Sprintf("[gosqstask] cannot reset message timeout visibility %s, "+
				"because most likely message has been already expired (error is not critical and ignored, "+
				"message will be processed next time, but for performance reasons our suggestion is "+
				"to increase Receiver.Concurrency value or/and increase Visibility Timeout in "+
				"SQS queue settings): %v", op, err))
		return true
	}
	return false
}

func newConcurrencyChannel(capacity int) chan struct{} {
	if capacity <= 0 {
		capacity = 1
	}
	cs := make(chan struct{}, capacity)
	for i := 0; i < capacity; i++ {
		cs <- struct{}{}
	}
	return cs
}
