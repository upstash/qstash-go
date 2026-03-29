package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ScheduleOption func(options *request) error

func (s *Schedules) WithScheduleId(scheduleId string) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set(upstashScheduleId, scheduleId)
		return nil
	}
}

func (s *Schedules) WithBody(body io.Reader) ScheduleOption {
	return func(opts *request) error {
		opts.body = body
		return nil
	}
}

func (s *Schedules) WithContentType(contentType string) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set("Upstash-Forward-Content-Type", contentType)
		return nil
	}
}

func (s *Schedules) WithMethod(method string) ScheduleOption {
	return func(options *request) error {
		options.header.Set(upstashMethodHeader, method)
		return nil
	}
}

func (s *Schedules) WithHeader(header http.Header) ScheduleOption {
	return func(opts *request) error {
		for k, v := range header {
			for _, vv := range v {
				opts.header.Add(fmt.Sprintf("Upstash-Forward-%s", k), vv)
			}
		}
		return nil
	}
}

func (s *Schedules) WithRetries(retries int) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set(upstashRetriesHeader, fmt.Sprintf("%d", retries))
		return nil
	}
}

func (s *Schedules) WithDelay(delay time.Duration) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set(upstashDelayHeader, delay.String())
		return nil
	}
}

func (s *Schedules) WithRetryDelay(retryDelay string) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set(upstashRetryDelayHeader, retryDelay)
		return nil
	}
}

func (s *Schedules) WithTimeout(timeout time.Duration) ScheduleOption {
	return func(opts *request) error {
		opts.header.Set(upstashTimeoutHeader, timeout.String())
		return nil
	}
}

func (s *Schedules) WithJSONBody(body any) ScheduleOption {
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
