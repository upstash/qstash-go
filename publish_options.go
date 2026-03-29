package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type PublishOption func(*request) error

func (c *Publish) WithContentType(contentType string) PublishOption {
	return func(opts *request) error {
		opts.header.Set("Upstash-Forward-Content-Type", contentType)
		return nil
	}
}

func (c *Publish) WithHeader(header http.Header) PublishOption {
	return func(opts *request) error {
		for k, v := range header {
			for _, vv := range v {
				opts.header.Add(fmt.Sprintf("Upstash-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Publish) WithMethod(method string) PublishOption {
	return func(opts *request) error {
		opts.header.Set(upstashMethodHeader, method)
		return nil
	}
}

func (c *Publish) WithTimeout(timeout time.Duration) PublishOption {
	return func(opts *request) error {
		opts.header.Set(upstashTimeoutHeader, timeout.String())
		return nil
	}
}

func (c *Publish) WithRetries(retry int) PublishOption {
	return func(opts *request) error {
		opts.header.Set(upstashRetriesHeader, fmt.Sprintf("%d", retry))
		return nil
	}
}

func (c *Publish) WithRetryDelay(retryDelay string) PublishOption {
	return func(opts *request) error {
		opts.header.Set(upstashRetryDelayHeader, retryDelay)
		return nil
	}
}

func (c *Publish) WithDelay(delay time.Duration) PublishOption {
	return func(opts *request) error {
		opts.header.Set(upstashDelayHeader, delay.String())
		return nil
	}
}

func (c *Publish) WithNotBefore(t time.Time) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashNotBefore, strconv.FormatInt(t.UnixMilli(), 10))
		return nil
	}
}

func (c *Publish) WithLabel(label string) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashLabelHeader, label)
		return nil
	}
}

func (c *Publish) WithFlowControlKey(flowControlKey string) PublishOption {
	return func(req *request) error {
		req.header.Set("Upstash-Flow-Control-Key", flowControlKey)
		return nil
	}
}

func (c *Publish) WithFlowControlParallelism(parallelism int) PublishOption {
	return func(req *request) error {
		flowControlValue := req.header.Get("Upstash-Flow-Control-Value")
		if flowControlValue != "" {
			flowControlValue += ","
		}
		flowControlValue += fmt.Sprintf("parallelism=%d", parallelism)
		req.header.Set("Upstash-Flow-Control-Value", flowControlValue)
		return nil
	}
}

func (c *Publish) WithFlowControlRatePeriod(rate int, period time.Duration) PublishOption {
	return func(req *request) error {
		flowControlValue := req.header.Get("Upstash-Flow-Control-Value")
		if flowControlValue != "" {
			flowControlValue += ","
		}
		flowControlValue += fmt.Sprintf("rate=%d", rate)
		flowControlValue += fmt.Sprintf("period=%d", int(period.Seconds()))
		return nil
	}
}

func (c *Publish) WithDeduplicationId(deduplicationId int) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashDeduplicationId, strconv.Itoa(deduplicationId))
		return nil
	}
}

func (c *Publish) WithContentBasedDeduplication(contentBasedDeduplication bool) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashContentBasedDeduplication, fmt.Sprintf("%t", contentBasedDeduplication))
		return nil
	}
}

func (c *Publish) WithCallback(callback string) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashCallbackHeader, callback)
		return nil
	}
}

func (c *Publish) WithCallbackHeader(callbackHeader http.Header) PublishOption {
	return func(req *request) error {
		for k, v := range callbackHeader {
			for _, vv := range v {
				req.header.Add(fmt.Sprintf("Upstash-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Publish) WithFailureCallback(failureCallback string) PublishOption {
	return func(req *request) error {
		req.header.Set(upstashFailureCallbackHeader, failureCallback)
		return nil
	}
}

func (c *Publish) WithFailureCallbackHeader(failureCallbackHeader http.Header) PublishOption {
	return func(req *request) error {
		for k, v := range failureCallbackHeader {
			for _, vv := range v {
				req.header.Add(fmt.Sprintf("Upstash-Failure-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Publish) WithBody(body io.Reader) PublishOption {
	return func(opts *request) error {
		opts.body = body
		return nil
	}
}

func (c *Publish) WithJSONBody(body any) PublishOption {
	return func(options *request) error {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		options.header.Set("Upstash-Forward-Content-Type", "application/json")
		options.body = bytes.NewReader(data)
		return nil
	}
}
