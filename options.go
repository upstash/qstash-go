package qstash

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func RetryCount(val int) *int {
	return &val
}

type PublishOptions struct {
	Url                       string
	UrlGroup                  string
	Api                       string
	Body                      string
	Method                    string
	ContentType               string
	Headers                   map[string]string
	Retries                   *int
	Callback                  string
	FailureCallback           string
	Forward                   string
	Delay                     string
	NotBefore                 string
	DeduplicationId           string
	ContentBasedDeduplication bool
	Timeout                   string
}

func (m PublishOptions) headers() http.Header {
	return prepareHeaders(
		m.ContentType,
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		m.NotBefore,
		m.DeduplicationId,
		m.ContentBasedDeduplication,
		m.Timeout,
		"",
	)
}

type PublishJSONOptions struct {
	Url                       string
	UrlGroup                  string
	Api                       string
	Body                      map[string]any
	Method                    string
	Headers                   map[string]string
	Retries                   *int
	Callback                  string
	FailureCallback           string
	Forward                   string
	Delay                     string
	NotBefore                 string
	DeduplicationId           string
	ContentBasedDeduplication bool
	Timeout                   string
}

func (m PublishJSONOptions) headers() http.Header {
	return prepareHeaders(
		"application/json",
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		m.NotBefore,
		m.DeduplicationId,
		m.ContentBasedDeduplication,
		m.Timeout,
		"",
	)
}

type EnqueueOptions struct {
	Queue string
	PublishOptions
}

func (m *EnqueueOptions) headers() http.Header {
	return prepareHeaders(
		m.ContentType,
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		m.NotBefore,
		m.DeduplicationId,
		m.ContentBasedDeduplication,
		m.Timeout,
		"",
	)
}

type EnqueueJSONOptions struct {
	Queue string
	PublishJSONOptions
}

func (m *EnqueueJSONOptions) headers() http.Header {
	return prepareHeaders(
		"application/json",
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		m.NotBefore,
		m.DeduplicationId,
		m.ContentBasedDeduplication,
		m.Timeout,
		"",
	)
}

type ScheduleOptions struct {
	Cron            string
	ContentType     string
	Body            string
	Destination     string
	Method          string
	Headers         map[string]string
	Retries         *int
	Callback        string
	FailureCallback string
	Delay           string
	Timeout         string
}

func (m *ScheduleOptions) headers() http.Header {
	return prepareHeaders(
		m.ContentType,
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		"",
		"",
		false,
		m.Timeout,
		m.Cron,
	)
}

type ScheduleJSONOptions struct {
	Cron            string
	Body            map[string]any
	Destination     string
	Method          string
	Headers         map[string]string
	Retries         *int
	Callback        string
	FailureCallback string
	Delay           string
	Timeout         string
}

func (m *ScheduleJSONOptions) headers() http.Header {
	return prepareHeaders(
		"application/json",
		m.Method,
		m.Headers,
		m.Retries,
		m.Callback,
		m.FailureCallback,
		m.Delay,
		"",
		"",
		false,
		m.Timeout,
		m.Cron,
	)
}

type BatchOptions struct {
	Queue                     string
	Url                       string
	UrlGroup                  string
	Api                       string
	Body                      string
	Method                    string
	ContentType               string
	Headers                   map[string]string
	Retries                   *int
	Callback                  string
	FailureCallback           string
	Forward                   string
	Delay                     string
	NotBefore                 string
	DeduplicationId           string
	ContentBasedDeduplication bool
	Timeout                   string
}

func (m *BatchOptions) headers() map[string]string {
	header := make(map[string]string)
	if m.ContentType != "" {
		header["Content-Type"] = m.ContentType
	}
	if m.Method != "" {
		header[upstashMethodHeader] = m.Method
	}
	for k, v := range m.Headers {
		if !strings.HasPrefix(strings.ToLower(k), "upstash-forward-") {
			k = fmt.Sprintf("%s-%s", upstashForwardHeader, k)
		}
		header[k] = v
	}
	if m.Retries != nil {
		header[upstashRetriesHeader] = fmt.Sprintf("%d", *m.Retries)
	}
	if m.Callback != "" {
		header[upstashCallbackHeader] = m.Callback
	}
	if m.FailureCallback != "" {
		header[upstashFailureCallbackHeader] = m.FailureCallback
	}
	if m.Delay != "" {
		header[upstashDelayHeader] = m.Delay
	}
	if m.NotBefore != "" {
		header[upstashNotBefore] = m.NotBefore
	}
	if m.DeduplicationId != "" {
		header[upstashDeduplicationId] = m.DeduplicationId
	}
	if m.ContentBasedDeduplication {
		header[upstashContentBasedDeduplication] = "true"
	}
	if m.Timeout != "" {
		header[upstashTimeoutHeader] = m.Timeout
	}
	return header
}

type BatchJSONOptions struct {
	Queue                     string
	Url                       string
	UrlGroup                  string
	Api                       string
	Body                      map[string]any
	Method                    string
	Headers                   map[string]string
	Retries                   *int
	Callback                  string
	FailureCallback           string
	Forward                   string
	Delay                     string
	NotBefore                 string
	DeduplicationId           string
	ContentBasedDeduplication bool
	Timeout                   string
}

func (m *BatchJSONOptions) headers() map[string]string {
	header := make(map[string]string)
	header["Content-Type"] = "application/json"
	if m.Method != "" {
		header[upstashMethodHeader] = m.Method
	}
	for k, v := range m.Headers {
		if !strings.HasPrefix(strings.ToLower(k), "upstash-forward-") {
			k = fmt.Sprintf("%s-%s", upstashForwardHeader, k)
		}
		header[k] = v
	}
	if m.Retries != nil {
		header[upstashRetriesHeader] = fmt.Sprintf("%d", *m.Retries)
	}
	if m.Callback != "" {
		header[upstashCallbackHeader] = m.Callback
	}
	if m.FailureCallback != "" {
		header[upstashFailureCallbackHeader] = m.FailureCallback
	}
	if m.Delay != "" {
		header[upstashDelayHeader] = m.Delay
	}
	if m.NotBefore != "" {
		header[upstashNotBefore] = m.NotBefore
	}
	if m.DeduplicationId != "" {
		header[upstashDeduplicationId] = m.DeduplicationId
	}
	if m.ContentBasedDeduplication {
		header[upstashContentBasedDeduplication] = "true"
	}
	if m.Timeout != "" {
		header[upstashTimeoutHeader] = m.Timeout
	}
	return header
}

type ListDlqOptions struct {
	// Cursor is the starting point for listing Dlq entries.
	Cursor string
	// Count is the maximum number of Dlq entries to return, default/maximum is 100.
	Count int
	// Filter is the filter to apply.
	Filter DlqFilter
}

func (l *ListDlqOptions) params() url.Values {
	params := url.Values{}
	if l.Cursor != "" {
		params.Set("cursor", l.Cursor)
	}
	if l.Count > 0 {
		params.Set("count", strconv.Itoa(l.Count))
	}
	if l.Filter.MessageId != "" {
		params.Set("messageId", l.Filter.MessageId)
	}
	if l.Filter.Url != "" {
		params.Set("url", l.Filter.Url)
	}
	if l.Filter.UrlGroup != "" {
		params.Set("topicName", l.Filter.UrlGroup)
	}
	if l.Filter.ScheduleId != "" {
		params.Set("scheduleId", l.Filter.ScheduleId)
	}
	if l.Filter.Queue != "" {
		params.Set("queueName", l.Filter.Queue)
	}
	if l.Filter.Api != "" {
		params.Set("api", l.Filter.Api)
	}
	if !l.Filter.FromDate.IsZero() {
		params.Set("fromDate", strconv.FormatInt(l.Filter.FromDate.UnixMilli(), 10))
	}
	if !l.Filter.ToDate.IsZero() {
		params.Set("toDate", strconv.FormatInt(l.Filter.ToDate.UnixMilli(), 10))
	}
	if l.Filter.ResponseStatus != 0 {
		params.Set("responseStatus", strconv.Itoa(l.Filter.ResponseStatus))
	}
	if l.Filter.CallerIP != "" {
		params.Set("callerIp", l.Filter.CallerIP)
	}
	return params
}

type ListEventsOptions struct {
	// Cursor is the starting point for listing events.
	Cursor string
	// Count is the maximum number of events to return.
	Count int
	// Filter is the filter to apply.
	Filter EventFilter
}

func (l *ListEventsOptions) Params() url.Values {
	params := url.Values{}
	if l.Cursor != "" {
		params.Set("cursor", l.Cursor)
	}
	if l.Count > 0 {
		params.Set("count", strconv.Itoa(l.Count))
	}
	if l.Filter.MessageId != "" {
		params.Set("messageId", l.Filter.MessageId)
	}
	if l.Filter.State != "" {
		params.Set("state", string(l.Filter.State))
	}
	if l.Filter.Url != "" {
		params.Set("url", l.Filter.Url)
	}
	if l.Filter.UrlGroup != "" {
		params.Set("topicName", l.Filter.UrlGroup)
	}
	if l.Filter.ScheduleId != "" {
		params.Set("scheduleId", l.Filter.ScheduleId)
	}
	if l.Filter.Queue != "" {
		params.Set("queueName", l.Filter.Queue)
	}
	if l.Filter.Api != "" {
		params.Set("api", l.Filter.Api)
	}
	if !l.Filter.FromDate.IsZero() {
		params.Set("fromDate", strconv.FormatInt(l.Filter.FromDate.UnixMilli(), 10))
	}
	if !l.Filter.ToDate.IsZero() {
		params.Set("toDate", strconv.FormatInt(l.Filter.ToDate.UnixMilli(), 10))
	}
	return params
}
