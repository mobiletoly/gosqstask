package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mobiletoly/gosqstask"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func main() {

	ctx := context.TODO()
	awscfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(fmt.Sprintf("Error loading AWS configuration: %v", err))
	}

	// TODO put your SQS URL here
	queue := "https://..."

	client := sqs.NewFromConfig(awscfg)
	recv := &gosqstask.Receiver{
		Client: client,
		// Receive one message at a time from SQS (good for balancing between multiple microservice instances)
		ReceiveMessageInput: &sqs.ReceiveMessageInput{
			// SQS queue URL
			QueueUrl: &queue,
			// Receive one message at a time from SQS (good for balancing between multiple microservice instances)
			MaxNumberOfMessages: 1,
			// Enable long polling (20 seconds is good in most cases)
			WaitTimeSeconds: 20, // Enable long polling
		},
		// Number of concurrent tasks to handle at once.
		// This is different from the number of messages received from SQS and represents
		// pool size. E.g. you can quickly receive 3 messages from SQS and then process
		// them concurrently. Once one of the tasks is finished, the next message will be
		// read from SQS and added to the pool.
		Concurrency: 3,
		// You can customize the per-message configuration. For example, you can check
		// if message you received requires long-running task and set AllowLongRunningTasks
		// to true. This will start a separate goroutine that will keep extending the
		// visibility timeout of the message in SQS.
		PerMessageConfig: func(_ context.Context, msg *types.Message, _ int) *gosqstask.PerMessageConfig {
			return &gosqstask.PerMessageConfig{
				AllowLongRunningTasks: true,
			}
		},
		// This is the function that will be called for each message received from SQS.
		Processor: func(ctx context.Context, msg *types.Message) error {
			defer func() {
				// Recover from panic and log it, without crashing the entire SQS listener
				// You might need it for more complicated message handling logic
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, fmt.Sprintf("Recovered from panic (messageId=%s): %v",
						*msg.MessageId, r))
				}
			}()

			slog.InfoContext(ctx, fmt.Sprintf("-------> RECEIVED %s / messageId=%s",
				*msg.Body, *msg.MessageId))
			time.Sleep(20 * time.Second) // Simulate long-running task
			slog.InfoContext(ctx, fmt.Sprintf("<------- PROCESSED %s / messageId=%s",
				*msg.Body, *msg.MessageId))
			return nil
		},
		// Optional logger. If not provided, it will use a default logger that
		// logs to stdout.
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	go func() {
		if err = recv.Listen(ctx); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	slog.Info("Server is shutting down")
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recv.Shutdown(ctx)
	slog.Info("Shutdown is complete")
}
