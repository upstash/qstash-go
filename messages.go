package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Messages struct {
	client *Client
}

type Message struct {
	// MessageId is the unique identifier of the message.
	MessageId string `json:"messageId"`
	// Endpoint is the endpoint name of the message if the endpoint is given a name within the url group.
	Endpoint string `json:"endpointName,omitempty"`
	// Url is the address to which the message should be delivered.
	Url string `json:"url,omitempty"`
	// UrlGroup is the url group name if this message was sent to an url group, empty otherwise.
	UrlGroup string `json:"urlGroup,omitempty"`
	// Method is the HTTP method to use for the message.
	Method string `json:"method"`
	// Header is the HTTP headers sent the endpoint.
	Header http.Header `json:"header"`
	// Body is the body of the message if it is composed of UTF-8 characters only, empty otherwise.
	Body string `json:"body,omitempty"`
	// BodyBase64 is the base64 encoded body if the body contains non-UTF-8 characters, empty otherwise.
	BodyBase64 string `json:"bodyBase64,omitempty"`
	// MaxRetries is the number of retries that should be attempted in case of delivery failure.
	MaxRetries int32 `json:"maxRetries"`
	// NotBefore is the unix timestamp in milliseconds before which the message should not be delivered.
	NotBefore int64 `json:"notBefore"`
	// CreatedAt is the unix timestamp in milliseconds when the message was created.
	CreatedAt int64 `json:"createdAt"`
	// Callback is the url which is called each time the message is attempted to be delivered.
	Callback string `json:"callback,omitempty"`
	// FailureCallback is the url which is called after the message is failed.
	FailureCallback string `json:"failureCallback,omitempty"`
	// ScheduleId is the id of scheduled job of the message if the message is triggered by a schedule.
	ScheduleId string `json:"scheduleId,omitempty"`
	// CallerIP is IP address of the publisher of this message.
	CallerIP string `json:"callerIP,omitempty"`
	// Queue is the queue name if this message was enqueued to a queue.
	Queue string `json:"queueName,omitempty"`
}

// Get gets the message by its id.
func (m *Messages) Get(messageId string) (Message, error) {
	opts := request{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/messages/%s", messageId),
	}
	response, _, err := m.client.fetchWith(opts)
	if err != nil {
		return Message{}, err
	}
	return parse[Message](response)
}

// Cancel cancels delivery of an existing message.
//
// Cancelling a message will remove it from QStash and stop it from being
// delivered in the future. If a message is in flight to your API,
// it might be too late to cancel.
func (m *Messages) Cancel(messageId string) error {
	opts := request{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/messages/%s", messageId),
	}
	_, _, err := m.client.fetchWith(opts)
	return err
}

// CancelMany cancels delivery of given messages.
func (m *Messages) CancelMany(messageIds []string) (int, error) {
	payload, err := json.Marshal(map[string][]string{"messageIds": messageIds})
	if err != nil {
		return 0, err
	}
	opts := request{
		method:      http.MethodDelete,
		path:        "/v2/messages",
		body:        bytes.NewReader(payload),
		contentType: "application/json",
	}
	response, _, err := m.client.fetchWith(opts)
	if err != nil {
		return 0, err
	}
	deleted, err := parse[map[string]int](response)
	if err != nil {
		return 0, err
	}
	return deleted["cancelled"], nil
}

// CancelAll cancels delivery of all existing messages.
func (m *Messages) CancelAll() (int, error) {
	opts := request{
		method:      http.MethodDelete,
		path:        "/v2/messages",
		contentType: "application/json",
	}
	response, _, err := m.client.fetchWith(opts)
	if err != nil {
		return 0, err
	}
	deleted, err := parse[map[string]int](response)
	if err != nil {
		return 0, err
	}
	return deleted["cancelled"], nil
}
