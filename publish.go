package qstash

import (
	"fmt"
	"net/http"
)

type MessageReceipt struct {
	// MessageId is the unique identifier of new message.
	MessageId string `json:"messageId"`
	// Deduplicated indicates whether the message is a duplicate that was not sent to the destination.
	Deduplicated bool `json:"deduplicated,omitempty"`
	// Url is the target address of the message if it was sent to a URL group, empty otherwise.
	Url string `json:"url,omitempty"`
}

type Publish func(destination string, o ...PublishOption) (MessageReceipt, error)

func publish(c *Client, destination string, o ...PublishOption) (MessageReceipt, error) {
	if destination == "" {
		return MessageReceipt{}, fmt.Errorf("destination cannot be empty")
	}
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", destination),
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
