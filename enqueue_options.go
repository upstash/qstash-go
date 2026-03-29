package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type EnqueueOption func(*request) error

func (c *Enqueue) WithContentType(contentType string) EnqueueOption {
	return func(req *request) error {
		req.header.Set("Content-Type", contentType)
		return nil
	}
}

func (c *Enqueue) WithHeader(header http.Header) EnqueueOption {
	return func(req *request) error {
		for k, v := range header {
			for _, vv := range v {
				req.header.Add(fmt.Sprintf("Upstash-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Enqueue) WithMethod(method string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashMethodHeader, method)
		return nil
	}
}

func (c *Enqueue) WithTimeout(timeout time.Duration) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashTimeoutHeader, timeout.String())
		return nil
	}
}

func (c *Enqueue) WithRetries(retry int) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashRetriesHeader, fmt.Sprintf("%d", retry))
		return nil
	}
}

func (c *Enqueue) WithRetryDelay(retryDelay string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashRetryDelayHeader, retryDelay)
		return nil
	}
}

func (c *Enqueue) WithLabel(label string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashLabelHeader, label)
		return nil
	}
}

func (c *Enqueue) WithDeduplicationId(deduplicationId string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashDeduplicationId, deduplicationId)
		return nil
	}
}

func (c *Enqueue) WithContentBasedDeduplication(contentBasedDeduplication bool) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashContentBasedDeduplication, fmt.Sprintf("%t", contentBasedDeduplication))
		return nil
	}
}

func (c *Enqueue) WithCallback(callback string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashCallbackHeader, callback)
		return nil
	}
}

func (c *Enqueue) WithCallbackHeader(callbackHeader http.Header) EnqueueOption {
	return func(req *request) error {
		for k, v := range callbackHeader {
			for _, vv := range v {
				req.header.Add(fmt.Sprintf("Upstash-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Enqueue) WithFailureCallback(failureCallback string) EnqueueOption {
	return func(req *request) error {
		req.header.Set(upstashFailureCallbackHeader, failureCallback)
		return nil
	}
}

func (c *Enqueue) WithFailureCallbackHeader(failureCallbackHeader http.Header) EnqueueOption {
	return func(req *request) error {
		for k, v := range failureCallbackHeader {
			for _, vv := range v {
				req.header.Add(fmt.Sprintf("Upstash-Failure-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (c *Enqueue) WithBody(body io.Reader) EnqueueOption {
	return func(opts *request) error {
		opts.body = body
		return nil
	}
}

func (c *Enqueue) WithJSONBody(body any) EnqueueOption {
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
