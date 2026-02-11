package qstash

import (
	"net/http"
)

// Readiness provides access to the readiness endpoint.
type Readiness struct {
	client *Client
}

// ReadinessResponse represents the response from the readiness endpoint.
type ReadinessResponse struct {
	// Ready indicates whether QStash is ready to accept requests.
	Ready bool `json:"ready"`
}

// Check retrieves the readiness status of QStash.
func (r *Readiness) Check() (ReadinessResponse, error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/readiness",
	}
	response, _, err := r.client.fetchWith(opts)
	if err != nil {
		return ReadinessResponse{}, err
	}
	readiness, err := parse[ReadinessResponse](response)
	return readiness, err
}
