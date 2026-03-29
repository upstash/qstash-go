package qstash

import (
	"bytes"
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
func (u *UrlGroups) Publish(urlGroup string, o ...PublishOption) ([]MessageReceipt, error) {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/publish/%s", urlGroup),
	}
	for _, opt := range o {
		err := opt(&opts)
		if err != nil {
			return nil, err
		}
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]MessageReceipt](response)
}

// Enqueue enqueues a message, after creating the queue if it does not exist.
func (u *UrlGroups) Enqueue(urlGroup string, queueName string, o ...EnqueueOption) ([]MessageReceipt, error) {
	opts := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/enqueue/%s/%s", queueName, urlGroup),
	}
	for _, opt := range o {
		err := opt(&opts)
		if err != nil {
			return nil, err
		}
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]MessageReceipt](response)
}

// UpsertEndpoints adds or updates one or more endpoints to an url group.
// If the url group or the endpoint does not exist, it will be created.
// If the endpoint exists, it will be updated.
func (u *UrlGroups) UpsertEndpoints(urlGroup string, endpoints []Endpoint) error {
	for _, endpoint := range endpoints {
		if endpoint.Url == "" {
			return fmt.Errorf("`url` of the endpoint must be provided")
		}
	}
	payload, err := json.Marshal(map[string][]Endpoint{
		"endpoints": endpoints,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	opts := request{
		method:      http.MethodPost,
		path:        fmt.Sprintf("/v2/topics/%s/endpoints", urlGroup),
		body:        bytes.NewReader(payload),
		contentType: "application/json",
	}
	_, _, err = u.client.fetchWith(opts)
	return err
}

// RemoveEndpoints removes one or more endpoints from an url group.
// If all endpoints have been removed, the url group will be deleted.
func (u *UrlGroups) RemoveEndpoints(urlGroup string, endpoints []Endpoint) error {
	for _, endpoint := range endpoints {
		if endpoint.Url == "" && endpoint.Name == "" {
			return fmt.Errorf("either `url` or `name` of the endpoint must be provided")
		}
	}
	payload, err := json.Marshal(map[string][]Endpoint{
		"endpoints": endpoints,
	})
	if err != nil {
		return err
	}
	opts := request{
		method:      http.MethodDelete,
		path:        fmt.Sprintf("/v2/topics/%s/endpoints", urlGroup),
		body:        bytes.NewReader(payload),
		contentType: "application/json",
	}
	_, _, err = u.client.fetchWith(opts)
	return err
}

// Get retrieves the url group by its name.
func (u *UrlGroups) Get(urlGroup string) (UrlGroup, error) {
	opts := request{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/topics/%s", urlGroup),
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return UrlGroup{}, err
	}
	return parse[UrlGroup](response)
}

// List retrieves all the url groups.
func (u *UrlGroups) List() ([]UrlGroup, error) {
	opts := request{
		method: http.MethodGet,
		path:   "/v2/topics",
	}
	response, _, err := u.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]UrlGroup](response)
}

// Delete deletes the url group and all its endpoints.
func (u *UrlGroups) Delete(urlGroup string) error {
	opts := request{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/topics/%s", urlGroup),
	}
	_, _, err := u.client.fetchWith(opts)
	return err
}
