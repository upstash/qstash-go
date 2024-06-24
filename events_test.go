package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEvents(t *testing.T) {
	client := NewClientWithEnv()

	now := time.Now()
	for i := 0; i < 100; i++ {
		res, err := client.PublishJSON(PublishJSONOptions{
			Url:     "http://httpstat.us/404",
			Retries: RetryCount(0),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, res.MessageId)
	}

	assert.Eventually(t, func() bool {
		subT := &testing.T{}

		events, _, err := client.Events().List(ListEventsOptions{
			Filter: EventFilter{
				FromDate: now,
			},
		})

		assert.NoError(subT, err)
		assert.Len(subT, events, 400)
		return !subT.Failed()
	}, time.Second*10, time.Millisecond*100)
}
