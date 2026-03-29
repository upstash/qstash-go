package qstash

import (
	"fmt"
	"net/http"
)

type Enqueue func(url string, queueName string, o ...EnqueueOption) (MessageReceipt, error)

func enqueue(c *Client, url string, queueName string, o ...EnqueueOption) (MessageReceipt, error) {
	if url == "" {
		return MessageReceipt{}, fmt.Errorf("destination cannot be empty")
	}
	if queueName == "" {
		return MessageReceipt{}, fmt.Errorf("queue name cannot be empty")
	}
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/enqueue/%s/%s", queueName, url),
	}
	for _, option := range o {
		err := option(&opts)
		if err != nil {
			return MessageReceipt{}, err
		}
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return MessageReceipt{}, err
	}
	return parse[MessageReceipt](response)
}
