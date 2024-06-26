# Upstash QStash Go SDK

QStash is an HTTP based messaging and scheduling solution for serverless and edge runtimes.

[QStash Documentation](https://upstash.com/docs/qstash)

## Install

Use go get to install the Upstash QStash package:

```
go get github.com/upstash/qstash-go
```

Import the Upstash QStash package in your project:

```
import "github.com/upstash/qstash-go"
```

## Usage

### Publish a JSON message

```
client := qstash.NewClient("<QSTASH_TOKEN>")

// Error checking is omitted for breavity
res, _ := client.PublishJSON(qstash.PublishJSONOptions{
    Url: "https://example.com",
    Body: map[string]any{
        "hello": "world",
    },
    Headers: map[string]string{
        "test-header": "test-value",
    },
})

fmt.Println(res.MessageId)
```

### Create a scheduled message

```
client := qstash.NewClient("<QSTASH_TOKEN>")

scheduleId, err := client.Schedules().Create(qstash.ScheduleOptions{
    Destination: "https://example.com",
    Cron: "*/5 * * * *",
})
// handle err

fmt.Print(scheduleId)
```

### Receiving messages

```
receiver := qstash.NewReceiver("<CURRENT_SIGNING_KEY>", "NEXT_SIGNING_KEY")

// ... in your request handler

signature := req.Header.Get("Upstash-Signature")
body, err := io.ReadAll(req.Body)
// handle err

err := receiver.Verify(qstash.VerifyOptions{
    Signature: signature,
    Body:      string(body),
    Url:       "https://example.com", // optional
})
// handle err
```

Additional methods are available for managing url groups, schedules, and messages.
