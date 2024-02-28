# GoSqsTask

GoSqsTask solves AWS SQS related tasks in Go. It supports:

- Customizable concurrency (to handle multiple tasks at once)
- Graceful shutdown with deadline cancellation of all tasks in progress.
- Automatic handling of long-running tasks. Library provides background task trackers to deal
  with visibility timeout of SQS queue messages and automatically extends it if task execution
  happens to be longer than expected visibility timeout. This prevents messages from being
  processed more than one by different consumer listeners.

## Installation

```bash
go get github.com/mobiletoly/gosqstask
```

## Usage

Here is a very simple example of how to use GoSQSTask to receive messages from SQS,
but make sure to refer to the documentation inside the receiver.go code for more
information about the configuration options.

```go
queue := "https://..."
client := sqs.NewFromConfig(awscfg)

recv := &gosqstask.Receiver{
    Client: client,
    ReceiveMessageInput: &sqs.ReceiveMessageInput{
        // SQS queue URL
        QueueUrl:            &queue,
        // Receive one message at a time from SQS
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
    // You can customize the per-message configuration. For example, you can check
    // if message you received requires long-running task and set AllowLongRunningTasks
    // to true. This will start a separate goroutine that will keep extending the
    // visibility timeout of the message in SQS.
    MessageConfig: func (_ context.Context, msg *types.Message, _ int) gosqstask.MessageConfig {
        return gosqstask.MessageConfig{
            ProcessRequest: gosqstask.ProcessRequestLongRunning,
        }
    },
    // This is the function that will be called for each message received from SQS.
    Processor: func (ctx context.Context, msg *types.Message, _ gosqstask.MessageConfig) error {
        slog.InfoContext(ctx, fmt.Sprintf("-------> RECEIVED %s / messageId=%s",
            *msg.Body, *msg.MessageId))
        time.Sleep(30 * time.Second) // Simulate long-running task
        slog.InfoContext(ctx, fmt.Sprintf("<------- PROCESSED %s / messageId=%s",
            *msg.Body, *msg.MessageId))
        return nil
    },
    // Optional logger. If not provided, it will use a default logger that
    // logs to stdout.
    Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
}

// Launch the receiver
go func() {
    if err = recv.Listen(ctx); err != nil {
        panic(err)
    }
}()

// React to Interrupt signal that can be received by pressing Ctrl+C,
// by stopping debugger or when process is getting terminated in the
// microservice instance. In this case we perform graceful shutdown.
// (read more about Shutdown method in receive.go source code comments)
quit := make(chan os.Signal, 1)
signal.Notify(quit, os.Interrupt)
<-quit
slog.Info("Server is shutting down")
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
recv.Shutdown(ctx)
slog.Info("Shutdown is complete")
```
