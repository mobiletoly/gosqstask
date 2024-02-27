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
	"time"
)

type ReceiverProcessor func(message *types.Message) error

type Receiver struct {
	// Client is the SQS client
	// This member is required.
	Client *sqs.Client
	// ReceiveMessageInput is the input for the ReceiveMessage operation
	// This member is required
	ReceiveMessageInput *sqs.ReceiveMessageInput
	// Concurrency is the number of concurrent workers to process messages
	// If this member is not set, default concurrency 1 will be used.
	Concurrency int
	// ReceiverProcessorConfig is the configuration for the Processor
	// Each received message will be passed to this function first to get the
	// per-message configuration. If this member is not set, default configuration
	// will be used:
	//		AllowLongRunningTasks=false
	PerMessageConfig func(ctx context.Context, msg *types.Message) *PerMessageConfig
	// Processor is the function to process the received message
	// This member is required
	Processor func(ctx context.Context, msg *types.Message) error
	// Logger
	Logger *slog.Logger
}

type PerMessageConfig struct {
	// AllowLongRunningTasks is a flag to allow long-running tasks, task can potentially take longer than
	// queue's Visibility Timeout. In this case Receiver will create a new background tracking task that will
	// be measuring the time since the SQS message was received and if it exceeds the Visibility Timeout,
	// then message's Visibility Timeout will be extended to remain invisible to other consumers and
	// in case of failure, the message will be visible again after new Visibility Timeout.
	// By default, this flag is set to false and does not allow long-running tasks
	AllowLongRunningTasks bool
}

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
		r.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
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
	for {
		result, err := r.Client.ReceiveMessage(ctx, r.ReceiveMessageInput)
		if err != nil {
			r.Logger.ErrorContext(ctx, "[gosqstask] failed to receive message from queue", err)
			continue
		}
		if len(result.Messages) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for i := range result.Messages {
			msg := &result.Messages[i]
			msgIdLog := slog.String("messageId", *msg.MessageId)
			beforeWait := time.Now()
			<-sync
			afterWait := time.Now()
			if afterWait.Sub(beforeWait) > time.Second*1 {
				r.Logger.DebugContext(ctx, "[gosqstask] reset message visibility after pool wait", msgIdLog)
				// It is possible that "<-sync" blocked message processing for some time,
				// and it is better to extend its visibility timeout before processing
				if err = r.changeMessageVisibility(ctx, msg.ReceiptHandle, queueVisTimeout); err != nil {
					if !r.isMessageExpiredError(ctx, err, msgIdLog, "after pool wait") {
						r.Logger.WarnContext(ctx,
							"[gosqstask] failed to reset message visibility timeout after pool wait "+
								"(error is not critical and ignored, message will be processed next time):",
							err, msgIdLog)
					}
					sync <- struct{}{}
					continue
				}
			}

			r.Logger.DebugContext(ctx, "[gosqstask] message received from SQS", msgIdLog)

			// Launch a new goroutine to process the message
			go func(msg *types.Message) {
				r.Logger.DebugContext(ctx, "[gosqstask] message is being processed", msgIdLog)
				defer func() { sync <- struct{}{} }()
				ctx, cancel := context.WithCancel(context.WithValue(ctx, "messageId", *msg.MessageId))

				var perMsgCfg *PerMessageConfig
				if r.PerMessageConfig != nil {
					perMsgCfg = r.PerMessageConfig(ctx, msg)
				} else {
					perMsgCfg = &PerMessageConfig{}
				}
				// If AllowLongRunningTasks is set to true, then create a new background tracking task
				if perMsgCfg.AllowLongRunningTasks {
					go func(ctx context.Context) {
						r.longRunningTaskTracker(ctx, msgIdLog, msg.ReceiptHandle, queueVisTimeout)
					}(ctx)
				}

				err = r.Processor(ctx, msg)
				if err != nil {
					r.Logger.ErrorContext(ctx, "[gosqstask] failed to process message:", err, msgIdLog)
				} else {
					_, err := r.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
						QueueUrl:      r.ReceiveMessageInput.QueueUrl,
						ReceiptHandle: msg.ReceiptHandle,
					})
					if err != nil {
						r.Logger.ErrorContext(ctx, "[gosqstask] failed to delete message from queue", err, msgIdLog)
					} else {
						r.Logger.DebugContext(ctx, "[gosqstask] message processed successfully", msgIdLog)
					}
				}
				cancel()
			}(msg)
		}
	}
}

func (r *Receiver) changeMessageVisibility(ctx context.Context, msgReceipt *string, visTimeout int) error {
	_, err := r.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          r.ReceiveMessageInput.QueueUrl,
		ReceiptHandle:     msgReceipt,
		VisibilityTimeout: int32(visTimeout),
	})
	return err
}

func (r *Receiver) longRunningTaskTracker(ctx context.Context, msgIdLog slog.Attr, msgReceipt *string, visTimeout int) {
	t := time.NewTicker(time.Second * time.Duration(visTimeout*3/4))
	defer t.Stop()
	r.Logger.DebugContext(ctx, "[gosqstask] long running task tracker is started", msgIdLog)
	for {
		select {
		case <-ctx.Done():
			r.Logger.DebugContext(ctx, "[gosqstask] shut down long running task tracker", msgIdLog)
			return
		case <-t.C:
			if ctx.Err() != nil {
				r.Logger.DebugContext(ctx, "[gosqstask] ignored changing message visibility timeout", msgIdLog)
				return
			}
			r.Logger.DebugContext(ctx,
				fmt.Sprintf("[gosqstask] extending message visibility timeout for %d seconds", visTimeout),
				msgIdLog)
			_, err := r.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          r.ReceiveMessageInput.QueueUrl,
				ReceiptHandle:     msgReceipt,
				VisibilityTimeout: int32(visTimeout),
			})
			if err != nil {
				if !r.isMessageExpiredError(ctx, err, msgIdLog, "in long running task tracker") {
					r.Logger.ErrorContext(ctx, "[gosqstask] failed to extend message visibility timeout "+
						"in long running task tracker", err, msgIdLog)
				}
			}
		}
	}
}

func (r *Receiver) isMessageExpiredError(ctx context.Context, err error, msgIdLog slog.Attr, op string) bool {
	var apiErr *smithy.GenericAPIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidParameterValue" {
		r.Logger.WarnContext(ctx,
			fmt.Sprintf("[gosqstask] cannot reset message timeout visibility %s, "+
				"because most likely message has been already expired (error is not critical and ignored, "+
				"message will be processed next time, but for performance reasons our suggestion is "+
				"to increase Receiver.Concurrency value or/and increase Visibility Timeout in "+
				"SQS queue settings):", op),
			err, msgIdLog)
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
