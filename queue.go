package qstash

import (
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

type QueueWithLag struct {
	// Name is the name of the queue.
	Name string `json:"name"`
	// Parallelism is the number of parallel consumers consuming from the queue.
	Parallelism int `json:"parallelism"`
	// CreatedAt is the creation time of the queue, in unix milliseconds.
	CreatedAt int64 `json:"createdAt"`
	// UpdatedAt is the last update time of the queue, in unix milliseconds
	UpdatedAt int64 `json:"updatedAt"`
	// Lag is the number of unprocessed messages that exist in the queue.
	Lag int64 `json:"lag"`
	// IsPaused is whether the queue is paused or not.
	IsPaused bool `json:"paused"`
}

type Queue struct {
	// Name is the name of the queue
	Name string `json:"queueName" validate:"required"`
	// Parallelism is the number of parallel consumers consuming from the queue.
	Parallelism int `json:"parallelism"`
	// IsPaused is whether the queue is paused or not.
	IsPaused bool `json:"paused"`
}

// Upsert updates or creates a queue.
func (c *Queues) Upsert(queue Queue) (err error) {
	payload, err := json.Marshal(queue)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   "/v2/queues",
		body:   string(payload),
		header: contentTypeJson,
	}
	_, _, err = c.client.fetchWith(opts)
	return
}

// Get retrieves a queue by its name.
func (c *Queues) Get(name string) (schedule QueueWithLag, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/queues/%s", name),
	}
	response, _, err := c.client.fetchWith(opts)
	if err != nil {
		return
	}
	schedule, err = parse[QueueWithLag](response)
	return
}

// List retrieves all queues.
func (c *Queues) List() (schedules []QueueWithLag, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/queues",
	}
	response, _, err := c.client.fetchWith(opts)
	if err != nil {
		return
	}
	schedules, err = parse[[]QueueWithLag](response)
	return
}

// Delete deletes a queue by its name.
func (c *Queues) Delete(queue string) (err error) {
	opts := requestOptions{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/queues/%s", queue),
	}
	_, _, err = c.client.fetchWith(opts)
	return
}

// Pause pauses the queue.
// A paused queue will not deliver messages until it is resumed.
func (c *Queues) Pause(queue string) (err error) {
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/queues/%s/pause", queue),
	}
	_, _, err = c.client.fetchWith(opts)
	return
}

// Resume resumes the queue.
func (c *Queues) Resume(queue string) (err error) {
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/queues/%s/resume", queue),
	}
	_, _, err = c.client.fetchWith(opts)
	return
}
