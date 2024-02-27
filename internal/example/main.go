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
	"time"
)

func main() {

	ctx := context.TODO()
	awscfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		println("Error loading AWS configuration:", err)
	}

	sqsc := sqs.NewFromConfig(awscfg)
	queue := "..." // TODO put your SQS URL here
	recv := &gosqstask.Receiver{
		Client: sqsc,
		ReceiveMessageInput: &sqs.ReceiveMessageInput{
			QueueUrl:            &queue,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20, // Enable long polling
		},
		Concurrency: 3,
		PerMessageConfig: func(_ context.Context, msg *types.Message) *gosqstask.PerMessageConfig {
			return &gosqstask.PerMessageConfig{
				AllowLongRunningTasks: true,
			}
		},
		Processor: func(ctx context.Context, msg *types.Message) error {
			slog.InfoContext(ctx, fmt.Sprintf("-------> RECEIVED %s / messageId=%s",
				*msg.Body, *msg.MessageId))
			time.Sleep(20 * time.Second)
			slog.InfoContext(ctx, fmt.Sprintf("<------- PROCESSED %s / messageId=%s",
				*msg.Body, *msg.MessageId))
			return nil
		},
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	if err = recv.Listen(ctx); err != nil {
		panic(err)
	}
}
