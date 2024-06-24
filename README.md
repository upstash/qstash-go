# Upstash QStash Go SDK

> [!NOTE]  
> **This project is in the Experimental Stage.**
> 
> We declare this project experimental to set clear expectations for your usage. There could be known or unknown bugs, the API could evolve, or the project could be discontinued if it does not find community adoption. While we cannot provide professional support for experimental projects, we’d be happy to hear from you if you see value in this project!


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

