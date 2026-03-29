package qstash

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type BatchOption func(*batchMessage) error

func (b *Batch) WithDestination(destination string) BatchOption {
	return func(opts *batchMessage) error {
		opts.Destination = destination
		return nil
	}
}

func (b *Batch) WithQueue(queue string) BatchOption {
	return func(opts *batchMessage) error {
		opts.Queue = queue
		return nil
	}
}

func (b *Batch) WithContentType(contentType string) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set("Upstash-Forward-Content-Type", contentType)
		return nil
	}
}

func (b *Batch) WithHeader(header http.Header) BatchOption {
	return func(opts *batchMessage) error {
		for k, v := range header {
			for _, vv := range v {
				opts.Headers.Add(fmt.Sprintf("Upstash-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (b *Batch) WithMethod(method string) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set(upstashMethodHeader, method)
		return nil
	}
}

func (b *Batch) WithTimeout(timeout time.Duration) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set(upstashTimeoutHeader, timeout.String())
		return nil
	}
}

func (b *Batch) WithRetries(retry int) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set(upstashRetriesHeader, fmt.Sprintf("%d", retry))
		return nil
	}
}

func (b *Batch) WithRetryDelay(retryDelay string) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set(upstashRetryDelayHeader, retryDelay)
		return nil
	}
}

func (b *Batch) WithDelay(delay time.Duration) BatchOption {
	return func(opts *batchMessage) error {
		opts.Headers.Set(upstashDelayHeader, delay.String())
		return nil
	}
}

func (b *Batch) WithNotBefore(t time.Time) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashNotBefore, strconv.FormatInt(t.UnixMilli(), 10))
		return nil
	}
}

func (b *Batch) WithLabel(label string) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashLabelHeader, label)
		return nil
	}
}

func (b *Batch) WithFlowControlKey(flowControlKey string) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set("Upstash-Flow-Control-Key", flowControlKey)
		return nil
	}
}

func (b *Batch) WithFlowControlParallelism(parallelism int) BatchOption {
	return func(req *batchMessage) error {
		flowControlValue := req.Headers.Get("Upstash-Flow-Control-Value")
		if flowControlValue != "" {
			flowControlValue += ","
		}
		flowControlValue += fmt.Sprintf("parallelism=%d", parallelism)
		req.Headers.Set("Upstash-Flow-Control-Value", flowControlValue)
		return nil
	}
}

func (b *Batch) WithFlowControlRatePeriod(rate int, period time.Duration) BatchOption {
	return func(req *batchMessage) error {
		flowControlValue := req.Headers.Get("Upstash-Flow-Control-Value")
		if flowControlValue != "" {
			flowControlValue += ","
		}
		flowControlValue += fmt.Sprintf("rate=%d", rate)
		flowControlValue += fmt.Sprintf("period=%d", int(period.Seconds()))
		return nil
	}
}

func (b *Batch) WithDeduplicationId(deduplicationId int) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashDeduplicationId, strconv.Itoa(deduplicationId))
		return nil
	}
}

func (b *Batch) WithContentBasedDeduplication(contentBasedDeduplication bool) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashContentBasedDeduplication, fmt.Sprintf("%t", contentBasedDeduplication))
		return nil
	}
}

func (b *Batch) WithCallback(callback string) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashCallbackHeader, callback)
		return nil
	}
}

func (b *Batch) WithCallbackHeader(callbackHeader http.Header) BatchOption {
	return func(req *batchMessage) error {
		for k, v := range callbackHeader {
			for _, vv := range v {
				req.Headers.Add(fmt.Sprintf("Upstash-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (b *Batch) WithFailureCallback(failureCallback string) BatchOption {
	return func(req *batchMessage) error {
		req.Headers.Set(upstashFailureCallbackHeader, failureCallback)
		return nil
	}
}

func (b *Batch) WithFailureCallbackHeader(failureCallbackHeader http.Header) BatchOption {
	return func(req *batchMessage) error {
		for k, v := range failureCallbackHeader {
			for _, vv := range v {
				req.Headers.Add(fmt.Sprintf("Upstash-Failure-Callback-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (b *Batch) WithBody(body io.Reader) BatchOption {
	return func(opts *batchMessage) error {
		data, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		opts.Body = data
		return nil
	}
}

func (b *Batch) WithJSONBody(body any) BatchOption {
	return func(options *batchMessage) error {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		options.Headers.Set("Upstash-Forward-Content-Type", "application/json")
		options.Body = data
		return nil
	}
}
