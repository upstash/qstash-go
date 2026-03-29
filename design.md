# Design Decisions

## Body Type

Current:
- `string`

Proposed:
- `[]byte`
- `io.Reader`

## Remove JSON methods

Does not make sense to have a JSON method.
Users do not benefit from type-safety.

- `map[string]any`

## Required Parameters

It is not clear which parameters are required.


1)
```
scheduleId, err := client.Schedules().Create(qstash.ScheduleOptions{
    Destination: "https://example.com",
    Cron: "*/5 * * * *",
})

scheduleId, err := client.Schedules().Create(qstash.ScheduleOptions{})
```

2) 
```
scheduleId, err := client.Schedules().Create("https://example.com", "*/5 * * * *", qstash.ScheduleOptions{
    Retries: 2,
})

scheduleId, err := client.Schedules().Create("https://example.com", "*/5 * * * *")
```

## Functional Parameters

1) 
```
res, err := client.Publish("https://example.com",
    WithBody(strings.NewReader("test-body")),
    WithHeader(http.Header{"test-header": []string{"test-value"}}),
    WithRetries(0),
)

res, err := client.Publish("https://example.com")
```

2)
```
res, err := client.Publish(PublishOptions{
    Body: strings.NewReader("test-body"),
    Url:  "https://example.com",
    Headers: http.Header{
        "test-header": []string{"test-value"},
    },
    Retries: RetryCount(0),
})
```