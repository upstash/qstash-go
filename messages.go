package qstash

import (
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
	// Api is the api name if this message was sent to an api.
	Api string `json:"api,omitempty"`
}

type PublishOrEnqueueResponse struct {
	// MessageId is the unique identifier of new message.
	MessageId string `json:"messageId"`
	// Deduplicated indicates whether the message is a duplicate that was not sent to the destination.
	Deduplicated bool `json:"deduplicated,omitempty"`
	// Url is the target address of the message if it was sent to a URL group, empty otherwise.
	Url string `json:"url,omitempty"`
}

// Publish publishes a message to QStash.
func (c *Client) Publish(options PublishOptions) (result PublishOrEnqueueResponse, err error) {
	if options.UrlGroup != "" {
		err = fmt.Errorf("use UrlGroups() client to publish a message to url group")
		return
	}
	destination, err := getDestination(options.Url, options.UrlGroup, options.Api)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", destination),
		header: options.headers(),
		body:   options.Body,
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[PublishOrEnqueueResponse](response)
	return
}

// PublishJSON publishes a message to QStash, automatically serializing the body as JSON string,
// and setting content type to `application/json`.
func (c *Client) PublishJSON(options PublishJSONOptions) (result PublishOrEnqueueResponse, err error) {
	if options.UrlGroup != "" {
		err = fmt.Errorf("use UrlGroups() client to publish a message to url group")
		return
	}
	destination, err := getDestination(options.Url, options.UrlGroup, options.Api)
	if err != nil {
		return
	}
	payload, err := json.Marshal(options.Body)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", destination),
		header: options.headers(),
		body:   string(payload),
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[PublishOrEnqueueResponse](response)
	return
}

// Enqueue enqueues a message, after creating the queue if it does not exist.
func (c *Client) Enqueue(options EnqueueOptions) (result PublishOrEnqueueResponse, err error) {
	if options.UrlGroup != "" {
		err = fmt.Errorf("use UrlGroups() client to enqueue a url group message")
		return
	}
	destination, err := getDestination(options.Url, options.UrlGroup, options.Api)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		header: options.headers(),
		path:   fmt.Sprintf("/v2/enqueue/%s/%s", options.Queue, destination),
		body:   options.Body,
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[PublishOrEnqueueResponse](response)
	return
}

// EnqueueJSON enqueues a message, after creating the queue if it does not exist.
// It automatically serializes the body as JSON string, and setting content type to `application/json`.
func (c *Client) EnqueueJSON(options EnqueueJSONOptions) (result PublishOrEnqueueResponse, err error) {
	if options.UrlGroup != "" {
		err = fmt.Errorf("use UrlGroups() client to enqueue a url group message")
		return
	}
	destination, err := getDestination(options.Url, options.UrlGroup, options.Api)
	if err != nil {
		return
	}
	payload, err := json.Marshal(options.Body)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/enqueue/%s/%s", options.Queue, destination),
		body:   string(payload),
		header: options.headers(),
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[PublishOrEnqueueResponse](response)
	return
}

// Batch publishes or enqueues multiple messages in a single request.
func (c *Client) Batch(options []BatchOptions) (result []PublishOrEnqueueResponse, err error) {
	messages := make([]map[string]interface{}, len(options))
	for idx, option := range options {
		destination, err := getDestination(option.Url, option.UrlGroup, option.Api)
		if err != nil {
			return nil, err
		}
		messages[idx] = map[string]interface{}{
			"destination": destination,
			"headers":     option.headers(),
			"body":        option.Body,
			"queue":       option.Queue,
		}
	}
	payload, err := json.Marshal(messages)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   "/v2/batch",
		body:   string(payload),
		header: map[string][]string{"Content-Type": {"application/json"}},
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// BatchJSON publishes or enqueues multiple messages in a single request,
// automatically serializing the message bodies as JSON strings, and setting content type to `application/json`.
func (c *Client) BatchJSON(options []BatchJSONOptions) (result []PublishOrEnqueueResponse, err error) {
	messages := make([]map[string]interface{}, len(options))

	for idx, option := range options {
		destination, err := getDestination(option.Url, option.UrlGroup, option.Api)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(option.Body)
		if err != nil {
			return nil, err
		}
		messages[idx] = map[string]interface{}{
			"destination": destination,
			"headers":     option.headers(),
			"body":        string(body),
			"queue":       option.Queue,
		}
	}
	payload, err := json.Marshal(messages)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   "/v2/batch",
		body:   string(payload),
		header: contentTypeJson,
	}
	response, _, err := c.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// Get gets the message by its id.
func (m *Messages) Get(messageId string) (message Message, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/messages/%s", messageId),
	}
	response, _, err := m.client.fetchWith(opts)
	if err != nil {
		return Message{}, err
	}
	message, err = parse[Message](response)
	return
}

// Cancel cancels delivery of an existing message.
//
// Cancelling a message will remove it from QStash and stop it from being
// delivered in the future. If a message is in flight to your API,
// it might be too late to cancel.
func (m *Messages) Cancel(messageId string) error {
	opts := requestOptions{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/messages/%s", messageId),
	}
	_, _, err := m.client.fetchWith(opts)
	return err
}
