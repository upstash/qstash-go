package qstash

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// UrlGroups in QStash are namespaces where you can publish messages that are then sent to multiple endpoints.
// After creating an url group, you can define multiple endpoints, each represented by a publicly available URL.
// When a message is published to an url group, it is distributed to all subscribed endpoints.
type UrlGroups struct {
	client *Client
}

type Endpoint struct {
	// Url is the target address of the endpoint.
	Url string `json:"url"`
	// Name is the optional name of the endpoint.
	Name string `json:"name,omitempty"`
}

type UrlGroup struct {
	// Name is the name of the url group.
	Name string `json:"name"`
	// CreatedAt is the creation time of the url group, in unix milliseconds.
	CreatedAt int64 `json:"createdAt"`
	// UpdatedAt is the last update time of the url group, in unix milliseconds.
	UpdatedAt int64 `json:"updatedAt"`
	// Endpoints is the list of endpoints belong to url group.
	Endpoints []Endpoint `json:"endpoints"`
}

// Publish publishes a message to QStash.
func (u *UrlGroups) Publish(po PublishOptions) (result []PublishOrEnqueueResponse, err error) {
	if po.UrlGroup == "" {
		err = fmt.Errorf("specify a url group to publish message")
		return
	}
	destination, err := getDestination(po.Url, po.UrlGroup, po.Api)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", destination),
		header: po.headers(),
		body:   po.Body,
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// PublishJSON publishes a message to QStash, automatically serializing the body as JSON string,
// and setting content type to `application/json`.
func (u *UrlGroups) PublishJSON(message PublishJSONOptions) (result []PublishOrEnqueueResponse, err error) {
	if message.UrlGroup == "" {
		err = fmt.Errorf("specify a url group to publish message")
		return
	}
	destination, err := getDestination(message.Url, message.UrlGroup, message.Api)
	if err != nil {
		return
	}
	payload, err := json.Marshal(message.Body)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", destination),
		header: message.headers(),
		body:   string(payload),
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// Enqueue enqueues a message, after creating the queue if it does not exist.
func (u *UrlGroups) Enqueue(options EnqueueOptions) (result []PublishOrEnqueueResponse, err error) {
	if options.UrlGroup == "" {
		err = fmt.Errorf("specify a url group to enqueue message")
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
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// EnqueueJSON enqueues a message, after creating the queue if it does not exist.
// It automatically serializes the body as JSON string, and setting content type to `application/json`.
func (u *UrlGroups) EnqueueJSON(message EnqueueJSONOptions) (result []PublishOrEnqueueResponse, err error) {
	if message.UrlGroup == "" {
		err = fmt.Errorf("specify a url group to enqueue message")
		return
	}
	destination, err := getDestination(message.Url, message.UrlGroup, message.Api)
	if err != nil {
		return
	}
	payload, err := json.Marshal(message.Body)
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/enqueue/%s/%s", message.Queue, destination),
		body:   string(payload),
		header: message.headers(),
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]PublishOrEnqueueResponse](response)
	return
}

// UpsertEndpoints adds or updates one or more endpoints to an url group.
// If the url group or the endpoint does not exist, it will be created.
// If the endpoint exists, it will be updated.
func (u *UrlGroups) UpsertEndpoints(urlGroup string, endpoints []Endpoint) (err error) {
	for _, endpoint := range endpoints {
		if endpoint.Url == "" {
			err = fmt.Errorf("`url` of the endpoint must be provided")
			return
		}
	}
	payload, err := json.Marshal(map[string][]Endpoint{
		"endpoints": endpoints,
	})
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/topics/%s/endpoints", urlGroup),
		body:   string(payload),
		header: contentTypeJson,
	}
	_, _, err = u.client.fetchWith(opts)
	return
}

// RemoveEndpoints removes one or more endpoints from an url group.
// If all endpoints have been removed, the url group will be deleted.
func (u *UrlGroups) RemoveEndpoints(urlGroup string, endpoints []Endpoint) (err error) {
	for _, endpoint := range endpoints {
		if endpoint.Url == "" && endpoint.Name == "" {
			err = fmt.Errorf("one of `url` or `name` of the endpoint must be provided")
			return
		}
	}
	payload, err := json.Marshal(map[string][]Endpoint{
		"endpoints": endpoints,
	})
	if err != nil {
		return
	}
	opts := requestOptions{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/topics/%s/endpoints", urlGroup),
		body:   string(payload),
		header: contentTypeJson,
	}
	_, _, err = u.client.fetchWith(opts)
	return
}

// Get retrieves the url group by its name.
func (u *UrlGroups) Get(urlGroup string) (result UrlGroup, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/topics/%s", urlGroup),
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[UrlGroup](response)
	if err != nil {
		return
	}
	return
}

// List retrieves all the url groups.
func (u *UrlGroups) List() (result []UrlGroup, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/topics",
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return
	}
	result, err = parse[[]UrlGroup](response)
	if err != nil {
		return
	}
	return
}

// Delete deletes the url group and all its endpoints.
func (u *UrlGroups) Delete(urlGroup string) (err error) {
	opts := requestOptions{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/topics/%s", urlGroup),
	}
	_, _, err = u.client.fetchWith(opts)
	return
}
