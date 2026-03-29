package qstash

import (
	"fmt"
	"net/http"
	"net/url"
)

type FlowControls struct {
	client *Client
}

type FlowControl struct {
	FlowControlKey      string `json:"flowControlKey"`
	WaitListSize        int    `json:"waitListSize"`
	ParallelismMax      int    `json:"parallelismMax"`
	ParallelismCount    int    `json:"parallelismCount"`
	RateMax             int    `json:"rateMax"`
	RateCount           int    `json:"rateCount"`
	RatePeriod          int    `json:"ratePeriod"`
	RatePeriodStart     int    `json:"ratePeriodStart"`
	IsPinnedParallelism bool   `json:"isPinnedParallelism"`
	IsPinnedRate        bool   `json:"isPinnedRate"`
	IsPaused            bool   `json:"isPaused"`
}

func (c *FlowControls) Get(flowControlKey string) (FlowControl, error) {
	opts := request{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/flowControl/%s", flowControlKey),
	}
	response, _, err := c.client.fetchWith(opts)
	if err != nil {
		return FlowControl{}, err
	}
	return parse[FlowControl](response)
}

func (c *FlowControls) List() ([]FlowControl, error) {
	opts := request{
		method: http.MethodGet,
		path:   "/v2/flowControl",
	}
	response, _, err := c.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]FlowControl](response)
}

func (c *FlowControls) Pause(flowControlKey string) error {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/flowControl/%s/pause", flowControlKey),
	}
	_, _, err := c.client.fetchWith(opts)
	return err
}

func (c *FlowControls) Resume(flowControlKey string) error {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/flowControl/%s/resume", flowControlKey),
	}
	_, _, err := c.client.fetchWith(opts)
	return err
}

func (c *FlowControls) ResetRate(flowControlKey string) error {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/flowControl/%s/resetRate", flowControlKey),
	}
	_, _, err := c.client.fetchWith(opts)
	return err
}

func (c *FlowControls) PinConfiguration(flowControlKey string, o ...FlowControlOption) error {
	if len(o) == 0 {
		return nil
	}
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/flowControl/%s/pin", flowControlKey),
	}
	for _, opt := range o {
		err := opt(&opts)
		if err != nil {
			return err
		}
	}
	_, _, err := c.client.fetchWith(opts)
	return err
}

func (c *FlowControls) UnpinConfiguration(flowControlKey string, parallelism bool, rate bool) error {
	if !parallelism && !rate {
		return nil
	}
	params := url.Values{}
	if parallelism {
		params.Set("parallelism", "true")
	}
	if rate {
		params.Set("rate", "true")
	}
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/flowControl/%s/unpin", flowControlKey),
		params: params,
	}
	_, _, err := c.client.fetchWith(opts)
	return err
}
