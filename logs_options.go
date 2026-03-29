package qstash

import (
	"strconv"
	"time"
)

type LogFilter struct {
	// MessageId filters events by the ID of the message.
	MessageId string
	// State filters events by the state of the message.
	State LogState
	// Url filters events by the URL of the message.
	Url string
	// UrlGroup filters events by URL group of the message.
	UrlGroup string
	// Queue filters events by queue name.
	Queue string
	// ScheduleId filters events by schedule ID.
	ScheduleId string
	// FromDate filters events by starting time in milliseconds.
	FromDate time.Time
	// ToDate filters events by ending time in milliseconds.
	ToDate time.Time
}

type LogsOptions func(request2 *request) error

func (l *Logs) WithCursor(cursor string) LogsOptions {
	return func(req *request) error {
		if cursor != "" {
			req.params.Set("cursor", cursor)
		}
		return nil
	}
}

func (l *Logs) WithCount(count int) LogsOptions {
	return func(req *request) error {
		if count > 0 {
			req.params.Set("count", strconv.Itoa(count))
		}
		return nil
	}
}

func (l *Logs) WithFilter(filter LogFilter) LogsOptions {
	return func(req *request) error {
		if filter.MessageId != "" {
			req.params.Set("messageId", filter.MessageId)
		}
		if filter.State != "" {
			req.params.Set("state", string(filter.State))
		}
		if filter.Url != "" {
			req.params.Set("url", filter.Url)
		}
		if filter.UrlGroup != "" {
			req.params.Set("topicName", filter.UrlGroup)
		}
		if filter.ScheduleId != "" {
			req.params.Set("scheduleId", filter.ScheduleId)
		}
		if filter.Queue != "" {
			req.params.Set("queueName", filter.Queue)
		}
		if !filter.FromDate.IsZero() {
			req.params.Set("fromDate", strconv.FormatInt(filter.FromDate.UnixMilli(), 10))
		}
		if !filter.ToDate.IsZero() {
			req.params.Set("toDate", strconv.FormatInt(filter.ToDate.UnixMilli(), 10))
		}
		return nil
	}
}
