package qstash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// Queues in QStash are mechanisms that ensure ordered delivery (FIFO) and allow controlled parallelism in processing messages.
// Messages are queued and delivered one by one in a first-in-first-out order, ensuring each message is processed before the next one is activated.
// If a message fails due to an endpoint returning a non-2xx code, retries are attempted before moving to the next message.
// To avoid overwhelming an endpoint and improve throughput, parallelism can be configured, allowing multiple messages to be processed concurrently.
type Queues struct {
	client *Client
}

type Queue struct {
	// Name is the name of the queue.
	Name string `json:"name"`
	// Parallelism is the number of parallel consumers consuming from the queue.
	Parallelism int `json:"parallelism"`
	// CreatedAt is the creation time of the queue, in unix milliseconds.
	CreatedAt int64 `json:"createdAt,omitempty"`
	// UpdatedAt is the last update time of the queue, in unix milliseconds
	UpdatedAt int64 `json:"updatedAt,omitempty"`
	// Lag is the number of unprocessed messages that exist in the queue.
	Lag int64 `json:"lag"`
	// IsPaused is whether the queue is paused or not.
	IsPaused bool `json:"paused"`
}

// Upsert updates or creates a queue.
func (q *Queues) Upsert(queueName string, opts ...QueueOption) error {
	options := &queueOptions{}
	for _, option := range opts {
		option(options)
	}
	options.QueueName = queueName
	payload, err := json.Marshal(options)
	if err != nil {
		return err
	}
	_, _, err = q.client.fetchWith(request{
		method:      http.MethodPost,
		path:        "/v2/queues",
		body:        bytes.NewReader(payload),
		contentType: "application/json",
	})
	return err
}

// Get retrieves a queue by its name.
func (q *Queues) Get(name string) (Queue, error) {
	opts := request{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/queues/%s", name),
	}
	data, _, err := q.client.fetchWith(opts)
	if err != nil {
		return Queue{}, err
	}
	return parse[Queue](data)
}

// List retrieves all queues.
func (q *Queues) List() ([]Queue, error) {
	opts := request{
		method: http.MethodGet,
		path:   "/v2/queues",
	}
	data, _, err := q.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]Queue](data)
}

// Delete deletes a queue by its name.
func (q *Queues) Delete(queueName string) (err error) {
	opts := request{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/queues/%s", queueName),
	}
	_, _, err = q.client.fetchWith(opts)
	return
}

// Pause pauses the queue.
// A paused queue will not deliver messages until it is resumed.
func (q *Queues) Pause(queueName string) error {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/queues/%s/pause", queueName),
	}
	_, _, err := q.client.fetchWith(opts)
	return err
}

// Resume resumes the queue.
func (q *Queues) Resume(queueName string) error {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/queues/%s/resume", queueName),
	}
	_, _, err := q.client.fetchWith(opts)
	return err
}
