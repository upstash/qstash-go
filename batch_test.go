package qstash

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	N := 3
	messages := make([][]BatchOption, N)

	for i := 0; i < N; i++ {
		messages[i] = []BatchOption{
			client.Batch.WithDestination("https://example.com"),
			client.Batch.WithBody(strings.NewReader(fmt.Sprintf("hi %d", i))),
			client.Batch.WithRetries(0),
			client.Batch.WithHeader(http.Header{
				fmt.Sprintf("test-header-%d", i): {fmt.Sprintf("test-value-%d", i)},
				"Content-Type":                   {"text/plain"},
			}),
		}
	}

	responses, err := client.Batch(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		assert.NotEmpty(t, response)
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}

	}
}

func TestBatchMixed(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	name := "go_url_group"

	err = client.UrlGroups.Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups.UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com", Name: "First endpoint"},
		{Url: "https://example.net", Name: "Second endpoint"},
	})
	assert.NoError(t, err)

	urlGroup, err := client.UrlGroups.Get(name)
	assert.NoError(t, err)
	assert.Equal(t, urlGroup.Name, name)
	assert.Len(t, urlGroup.Endpoints, 2)

	N := 3
	messages := make([][]BatchOption, N)

	for i := 0; i < N; i++ {
		if i%2 == 0 {
			messages[i] = []BatchOption{
				client.Batch.WithRetries(1),
				client.Batch.WithDestination(name),
				client.Batch.WithBody(strings.NewReader(fmt.Sprintf("hi %d", i))),
				client.Batch.WithRetries(0),
				client.Batch.WithHeader(http.Header{
					fmt.Sprintf("test-header-%d", i): {fmt.Sprintf("test-value-%d", i)},
					"Content-Type":                   {"text/plain"},
				}),
			}
		} else {
			messages[i] = []BatchOption{
				client.Batch.WithBody(strings.NewReader(fmt.Sprintf("hi %d", i))),
				client.Batch.WithRetries(0),
				client.Batch.WithDestination("https://example.com"),
				client.Batch.WithHeader(http.Header{
					fmt.Sprintf("test-header-%d", i): {fmt.Sprintf("test-value-%d", i)},
					"Content-Type":                   {"text/plain"},
				}),
			}
		}

	}

	responses, err := client.Batch(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}
	}

	err = client.UrlGroups.Delete(name)
	assert.NoError(t, err)
}
