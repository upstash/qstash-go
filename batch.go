package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type batchMessage struct {
	Destination string      `json:"destination"`
	Headers     http.Header `json:"headers"`
	Body        interface{} `json:"body"`
	Queue       string      `json:"queue"`
}

type Batch func(options [][]BatchOption) (results [][]MessageReceipt, err error)

// Batch publishes or enqueues multiple messages in a single request.
func batch(c *Client, options [][]BatchOption) (results [][]MessageReceipt, err error) {
	messages := make([]batchMessage, len(options))
	for idx, o := range options {
		for _, p := range o {
			err := p(&messages[idx])
			if err != nil {
				return nil, err
			}
		}
	}
	payload, err := json.Marshal(messages)
	if err != nil {
		return
	}
	opts := request{
		method:      http.MethodPost,
		path:        "/v2/batch",
		body:        bytes.NewReader(payload),
		contentType: "application/json",
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err := parse[batchResponse](response)
	if err != nil {
		return nil, err
	}
	return result.responses, err
}

type batchResponse struct {
	responses [][]MessageReceipt
}

func (b *batchResponse) UnmarshalJSON(data []byte) (err error) {
	switch {
	case len(data) == 0 || string(data) == `null`:
		return nil
	case data[0] == '[':
		var t []interface{}
		if err = json.Unmarshal(data, &t); err != nil {
			return
		}
		for _, v := range t {
			if _, ok := v.([]interface{}); ok {
				var result []MessageReceipt
				response, jErr := json.Marshal(v)
				if jErr != nil {
					return
				}
				if err = json.Unmarshal(response, &result); err != nil {
					return
				}
				b.responses = append(b.responses, result)
			} else {
				var result MessageReceipt
				response, jErr := json.Marshal(v)
				if jErr != nil {
					return
				}
				if err = json.Unmarshal(response, &result); err != nil {
					return
				}
				b.responses = append(b.responses, []MessageReceipt{result})
			}
		}
	default:
		return fmt.Errorf("unsupported json type")
	}
	return nil
}
