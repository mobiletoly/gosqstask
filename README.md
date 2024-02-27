# GoSQSTask

GoSQSTask is a library for handling AWS SQS tasks in Go. It supports customizable
concurrency (to handle multiple tasks at once), and provides an automatic support
for handling long-running tasks (tasks that take longer than the visibility timeout).
Framework takes care of the visibility timeout in SQS queues and automatically extends
it if needed.

## Installation

```bash
go get github.com/mobiletoly/gosqstask
```

## Usage

```go
queue := "https://..."
client := sqs.NewFromConfig(awscfg)

recv := &gosqstask.Receiver{
    Client: client,
    ReceiveMessageInput: &sqs.ReceiveMessageInput{
        // SQS queue URL
        QueueUrl:            &queue,
        // Receive one message at a time from SQS (good for balancing between multiple microservice instances)
        MaxNumberOfMessages: 1,
        // Enable long polling (20 seconds is good in most cases)
        WaitTimeSeconds:     20,
    },
    // Number of concurrent tasks to handle at once.
    // This is different from the number of messages received from SQS and represents
    // pool size. E.g. you can quickly receive 3 messages from SQS and then process
    // them concurrently. Once one of the tasks is finished, the next message will be
    // read from SQS and added to the pool.
    Concurrency: 3,
    // You can customize the per-message configuration. For example you can check
    // if message you received requires long-running task and set AllowLongRunningTasks
    // to true. This will start a separate goroutine that will keep extending the
    // visibility timeout of the message in SQS.
    PerMessageConfig: func (_ context.Context, msg *types.Message) *gosqstask.PerMessageConfig {
        return &gosqstask.PerMessageConfig{
            AllowLongRunningTasks: true,
        }
    },
    // This is the function that will be called for each message received from SQS.
    Processor: func (ctx context.Context, msg *types.Message) error {
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

// Launch the receiver
if err = recv.Listen(ctx); err != nil {
    panic(err)
}

```
