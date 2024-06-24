package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDlqGetAndDelete(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.PublishJSON(PublishJSONOptions{
		Url:     "http://httpstat.us/404",
		Retries: RetryCount(1),
	})

	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	dlqIds := AssertFailedEventually(t, client, res.MessageId)

	err = client.Dlq().Delete(dlqIds[0])
	assert.NoError(t, err)
}

func TestDlqGetAndDeleteMany(t *testing.T) {
	client := NewClientWithEnv()

	messageIds := make([]string, 3)

	for i := 0; i < 3; i++ {
		res, err := client.PublishJSON(PublishJSONOptions{
			Url:     "http://httpstat.us/404",
			Retries: RetryCount(1),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, res.MessageId)
		messageIds[i] = res.MessageId
	}

	dlqIds := AssertFailedEventually(t, client, messageIds...)

	deleted, err := client.Dlq().DeleteMany(dlqIds)
	assert.NoError(t, err)
	assert.Equal(t, len(messageIds), deleted)
}

func AssertFailedEventually(t *testing.T, client *Client, messageIds ...string) (dlqIds []string) {
	dlqIds = make([]string, len(messageIds))
	assert.Eventually(t, func() bool {

		subT := &testing.T{}

		for idx, messageId := range messageIds {

			dlqMessages, _, err := client.Dlq().List(ListDlqOptions{
				Filter: DlqFilter{
					MessageId: messageId,
				},
			})
			assert.NoError(subT, err)
			assert.Len(subT, dlqMessages, 1)

			if !subT.Failed() {
				match := dlqMessages[0]
				res, err := client.Dlq().Get(match.DlqId)
				assert.NoError(subT, err)
				assert.Equal(subT, "404 Not Found", res.ResponseBody)
				assert.Equal(subT, "404 Not Found", match.ResponseBody)
				dlqIds[idx] = match.DlqId
			}
		}
		return !subT.Failed()
	}, time.Second*30, time.Millisecond*100)
	return
}
