package qstash

import (
	"fmt"
	"time"
)

type FlowControlOption func(*request) error

func (f *FlowControls) WithParallelism(parallelism int) FlowControlOption {
	return func(opts *request) error {
		opts.params.Set("parallelism", fmt.Sprintf("%d", parallelism))
		return nil
	}
}

func (f *FlowControls) WithRate(rate int) FlowControlOption {
	return func(opts *request) error {
		opts.params.Set("rate", fmt.Sprintf("%d", rate))
		return nil
	}
}

func (f *FlowControls) WithPeriod(period time.Duration) FlowControlOption {
	return func(opts *request) error {
		opts.params.Set("period", fmt.Sprintf("%d", int(period.Seconds())))
		return nil
	}
}
