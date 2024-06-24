package qstash

import (
	"net/http"
)

type Keys struct {
	client *Client
}

type SigningKeys struct {
	Current string `json:"current"`
	Next    string `json:"next"`
}

// Get retrieves the current and next signing keys.
func (k *Keys) Get() (keys SigningKeys, err error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/keys",
	}
	response, _, err := k.client.fetchWith(opts)
	if err != nil {
		return
	}
	keys, err = parse[SigningKeys](response)
	return
}

// Rotate rotates the current signing key and gets the new signing key.
// The next signing key becomes the current signing key, and a new signing key is assigned to the next signing key.
func (k *Keys) Rotate() (keys SigningKeys, err error) {
	opts := requestOptions{
		method: http.MethodPost,
		path:   "/v2/rotate",
	}
	response, _, err := k.client.fetchWith(opts)
	if err != nil {
		return
	}
	keys, err = parse[SigningKeys](response)
	return
}
