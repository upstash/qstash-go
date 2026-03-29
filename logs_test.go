package qstash

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvents(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	now := time.Now()
	for i := 0; i < 100; i++ {
		res, err := client.Publish("https://httpstat.us/404", client.Publish.WithRetries(0))
		assert.NoError(t, err)
		assert.NotEmpty(t, res.MessageId)
	}

	assert.Eventually(t, func() bool {
		subT := &testing.T{}

		events, _, err := client.Logs.List(
			client.Logs.WithFilter(LogFilter{
				FromDate: now,
			}),
		)

		assert.NoError(subT, err)
		assert.Len(subT, events, 400)
		return !subT.Failed()
	}, time.Second*10, time.Millisecond*100)
}
