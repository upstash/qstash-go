package qstash

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublishToUrl(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	res, err := client.Publish("https://example.com",
		client.Publish.WithBody(strings.NewReader("test-body")),
		client.Publish.WithHeader(http.Header{"test-header": []string{"test-value"}}),
		client.Publish.WithRetries(0),
	)

	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func TestPublishWithDelay(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	res, err := client.Publish("https://example.com",
		client.Publish.WithBody(strings.NewReader("test-body")),
		client.Publish.WithHeader(http.Header{"test-header": []string{"test-value"}}),
		client.Publish.WithDelay(time.Second*10),
		client.Publish.WithRetries(0),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	message, err := client.Messages.Get(res.MessageId)
	assert.NoError(t, err)
	assert.Equal(t, "test-body", message.Body)
	assert.Equal(t, "https://example.com", message.Url)
	assert.Equal(t, http.Header{"Test-Header": []string{"test-value"}}, message.Header)

}

func TestPublishToJson(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	body := map[string]string{
		"ex_key": "ex_value",
	}
	res, err := client.Publish("https://example.com",
		client.Publish.WithJSONBody(body),
		client.Publish.WithHeader(http.Header{"test-header": []string{"test-value"}}),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func TestTimeout(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	res, err := client.Publish("https://example.com",
		client.Publish.WithJSONBody(map[string]any{"ex_key": "ex_value"}),
		client.Publish.WithTimeout(time.Second),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}
