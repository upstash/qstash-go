package qstash

import (
	"strconv"
)

type DlqOption func(options *request) error

func (d *Dlq) WithCursor(cursor string) DlqOption {
	return func(req *request) error {
		if cursor != "" {
			req.params.Set("cursor", cursor)
		}
		return nil
	}
}

func (d *Dlq) WithCount(count int) DlqOption {
	return func(req *request) error {
		if count > 0 {
			req.params.Set("count", strconv.Itoa(count))
		}
		return nil
	}
}

func (d *Dlq) WithFilter(filter DlqFilter) DlqOption {
	return func(req *request) error {
		if filter.MessageId != "" {
			req.params.Set("messageId", filter.MessageId)
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
		if filter.ResponseStatus != 0 {
			req.params.Set("responseStatus", strconv.Itoa(filter.ResponseStatus))
		}
		if filter.CallerIP != "" {
			req.params.Set("callerIp", filter.CallerIP)
		}
		return nil
	}
}
