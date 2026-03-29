package qstash

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	res, err := client.Enqueue("https://example.com", "test-queue",
		client.Enqueue.WithBody(strings.NewReader("test-body")),
		client.Enqueue.WithHeader(http.Header{"test-header": []string{"test-value"}}),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestEnqueueJSON(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	res, err := client.Enqueue("https://example.com", "test-queue",
		client.Enqueue.WithJSONBody(map[string]string{"test": "body"}),
		client.Enqueue.WithHeader(http.Header{"test-header": []string{"test-value"}}),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}
