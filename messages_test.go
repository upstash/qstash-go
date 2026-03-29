package qstash

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCancelMany(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	var messageIds []string

	for i := 0; i < 10; i++ {
		res, err := client.Publish("https://example.com",
			client.Publish.WithJSONBody(map[string]any{"ex_key": "ex_value"}),
			client.Publish.WithDelay(time.Second*60),
		)
		assert.NoError(t, err)
		if i%2 == 0 {
			assert.NotEmpty(t, res.MessageId)
			messageIds = append(messageIds, res.MessageId)
		}
	}
	deleted, err := client.Messages.CancelMany(messageIds)
	assert.NoError(t, err)
	assert.Equal(t, 5, deleted)
}

func TestCancelAll(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		res, err := client.Publish("https://httpstat.us/400",
			client.Publish.WithJSONBody(map[string]any{"ex_key": "ex_value"}),
			client.Publish.WithDelay(time.Second*30),
			client.Publish.WithRetries(0),
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, res.MessageId)
	}

	time.Sleep(1 * time.Second)
	deleted, err := client.Messages.CancelAll()
	assert.NoError(t, err)
	assert.Greater(t, deleted, 0)
}

func AssertDeliveredEventually(t *testing.T, client *Client, messageId string) {
	assert.Eventually(t, func() bool {
		subT := &testing.T{}

		events, _, err := client.Logs.List(
			client.Logs.WithFilter(LogFilter{
				MessageId: messageId,
				State:     Delivered,
			}),
		)

		assert.NoError(subT, err)
		assert.Len(subT, events, 1)

		return !subT.Failed()
	}, time.Second*30, 100*time.Millisecond)
}
