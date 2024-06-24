package qstash

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Dlq (Dead Letter Queue) is a specialized queue used to store messages that cannot be processed successfully by the API.
// When the API fails to process a request due to reasons like bugs in the code, temporary issues with third-party services, or network problems, QStash will retry processing the message a few times.
// If the retries are unsuccessful, the message is then moved to the Dlq.
// This allows for these problematic messages to be handled manually, ensuring they don't get lost or cause further issues in the system.
type Dlq struct {
	client *Client
}

type DlqMessage struct {
	Message
	// DlqId is the unique id within the Dlq.
	DlqId string `json:"dlqId"`
	// ResponseStatus is the HTTP status code of the last failed delivery attempt.
	ResponseStatus int `json:"responseStatus,omitempty"`
	// ResponseHeaders is the response headers of the last failed delivery attempt.
	ResponseHeaders http.Header `json:"responseHeader,omitempty"`
	// ResponseBody is the response body of the last failed delivery attempt if it is composed of UTF-8 characters only, empty otherwise.
	ResponseBody string `json:"responseBody,omitempty"`
	// ResponseBodyBase64 is the base64 encoded response body of the last failed delivery attempt if the response body contains non-UTF-8 characters, empty otherwise.
	ResponseBodyBase64 string `json:"responseBodyBase64,omitempty"`
}

type DlqFilter struct {
	// MessageId filters Dlq entries by the ID of the message.
	MessageId string
	// Url filters Dlq entries by the URL of the message.
	Url string
	// UrlGroup filters Dlq entries by URL group of the message.
	UrlGroup string
	// ScheduleId filters Dlq entries by schedule ID.
	ScheduleId string
	// Queue filters Dlq entries by queue name.
	Queue string
	// Api filters Dlq entries by the API name of the message.
	Api string
	// FromDate filters Dlq entries by starting time in milliseconds.
	FromDate time.Time
	// ToDate filters Dlq entries by ending time in milliseconds.
	ToDate time.Time
	// ResponseStatus filters Dlq entries by HTTP response status code of the message.
	ResponseStatus int
	// CallerIP filters Dlq entries by IP address of the publisher of the message.
	CallerIP string
}

type listDlqResponse struct {
	Cursor   string       `json:"cursor,omitempty"`
	Messages []DlqMessage `json:"messages"`
}

// Get retrieves a message from the DLQ by its unique ID.
func (d *Dlq) Get(dlqId string) (dlqMessage DlqMessage, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/dlq/%s", dlqId),
	}
	response, _, err := d.client.fetchWith(opts)
	if err != nil {
		return
	}
	dlqMessage, err = parse[DlqMessage](response)
	return
}

// List retrieves all messages currently in the Dlq.
func (d *Dlq) List(options ListDlqOptions) (messages []DlqMessage, cursor string, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/dlq",
		params: options.params(),
	}
	response, _, err := d.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err := parse[listDlqResponse](response)
	if err != nil {
		return
	}
	return result.Messages, result.Cursor, nil
}

// Delete deletes a message from the Dlq by its unique ID.
func (d *Dlq) Delete(dlqId string) error {
	opts := requestOptions{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/dlq/%s", dlqId),
	}
	_, _, err := d.client.fetchWith(opts)
	return err
}

// DeleteMany deletes multiple messages from the Dlq and returns the number of deleted messages.
func (d *Dlq) DeleteMany(dlqIds []string) (int, error) {
	payload, err := json.Marshal(map[string][]string{"dlqIds": dlqIds})
	if err != nil {
		return 0, err
	}
	opts := requestOptions{
		method: http.MethodDelete,
		path:   "/v2/dlq",
		body:   string(payload),
		header: map[string][]string{"Content-Type": {"application/json"}},
	}
	response, _, err := d.client.fetchWith(opts)
	if err != nil {
		return 0, err
	}
	deleted, err := parse[map[string]int](response)
	if err != nil {
		return 0, err
	}
	return deleted["deleted"], nil
}
