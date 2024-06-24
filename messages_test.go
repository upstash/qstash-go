package qstash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestPublishToUrl(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.Publish(PublishOptions{
		Body: "test-body",
		Url:  "http://example.com",
		Headers: map[string]string{
			"test-header": "test-value",
		},
		Retries: RetryCount(0),
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func TestPublishToUrlWithDelay(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.Publish(PublishOptions{
		Body: "test-body",
		Url:  "http://example.com",
		Headers: map[string]string{
			"test-header": "test-value",
		},
		Retries: RetryCount(0),
		Delay:   "10s",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	message, err := client.Messages().Get(res.MessageId)
	assert.NoError(t, err)
	assert.Equal(t, "test-body", message.Body)
	assert.Equal(t, "http://example.com", message.Url)
	assert.Equal(t, http.Header{"Test-Header": []string{"test-value"}}, message.Header)

}

func TestPublishToJson(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.PublishJSON(PublishJSONOptions{
		Body: map[string]any{
			"ex_key": "ex_value",
		},
		Url: "https://example.com",
		Headers: map[string]string{
			"test-header": "test-value",
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func TestDisallowMultipleDestinations(t *testing.T) {
	client := NewClientWithEnv()

	_, err := client.Publish(PublishOptions{
		Url: "https://example.com",
		Api: "llm",
	})
	assert.ErrorContains(t, err, "multiple destinations found")

	_, err = client.Publish(PublishOptions{
		Url:      "https://example.com",
		UrlGroup: "test-url-group",
	})
	assert.ErrorContains(t, err, "use UrlGroups() client")

	_, err = client.Publish(PublishOptions{
		UrlGroup: "test-url-group",
		Api:      "llm",
	})
	assert.ErrorContains(t, err, "use UrlGroups() client")
}

func TestBatch(t *testing.T) {
	client := NewClientWithEnv()

	N := 3
	messages := make([]BatchOptions, N)

	for i := 0; i < N; i++ {
		messages[i] = BatchOptions{
			Body:    fmt.Sprintf("hi %d", i),
			Url:     "https://example.com",
			Retries: RetryCount(0),
			Headers: map[string]string{
				fmt.Sprintf("test-header-%d", i): fmt.Sprintf("test-value-%d", i),
				"Content-Type":                   "text/plain",
			},
		}
	}

	responses, err := client.Batch(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		assert.NotEmpty(t, response.MessageId)
	}
}

func TestBatchJSON(t *testing.T) {
	client := NewClientWithEnv()

	N := 3
	messages := make([]BatchJSONOptions, N)

	for i := 0; i < N; i++ {
		messages[i] = BatchJSONOptions{
			Body:    map[string]any{"hi": i},
			Url:     "https://example.com",
			Retries: RetryCount(0),
			Headers: map[string]string{
				fmt.Sprintf("test-header-%d", i): fmt.Sprintf("test-value-%d", i),
			},
		}
	}

	responses, err := client.BatchJSON(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		assert.NotEmpty(t, response.MessageId)
	}
}

func TestPublishToLlmApi(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.PublishJSON(PublishJSONOptions{
		Api: "llm",
		Body: map[string]any{
			"model": "meta-llama/Meta-Llama-3-8B-Instruct",
			"messages": []map[string]string{
				{
					"role":    "user",
					"content": "hello",
				},
			},
		},
		Callback: "http://example.com",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func TestBatchLlmApi(t *testing.T) {
	client := NewClientWithEnv()

	messages, err := client.BatchJSON([]BatchJSONOptions{
		{
			Api: "llm",
			Body: map[string]any{
				"model": "meta-llama/Meta-Llama-3-8B-Instruct",
				"messages": []map[string]string{
					{
						"role":    "user",
						"content": "hello",
					},
				},
			},
			Callback: "http://example.com",
		},
	})
	assert.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.NotEmpty(t, messages[0].MessageId)

	AssertDeliveredEventually(t, client, messages[0].MessageId)
}

func TestEnqueue(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.Enqueue(EnqueueOptions{
		Queue: "test-queue",
		PublishOptions: PublishOptions{
			Body: "test-body",
			Url:  "https://example.com",
			Headers: map[string]string{
				"test-header": "test-value",
			},
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestEnqueueJSON(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.EnqueueJSON(EnqueueJSONOptions{
		Queue: "test-queue",
		PublishJSONOptions: PublishJSONOptions{
			Body: map[string]any{"test": "body"},
			Url:  "https://example.com",
			Headers: map[string]string{
				"test-header": "test-value",
			},
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestEnqueueLlmApi(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.EnqueueJSON(EnqueueJSONOptions{
		Queue: "test-queue",
		PublishJSONOptions: PublishJSONOptions{
			Api: "llm",
			Body: map[string]any{
				"model": "meta-llama/Meta-Llama-3-8B-Instruct",
				"messages": []map[string]string{
					{
						"role":    "user",
						"content": "hello",
					},
				},
			},
			Callback: "http://example.com",
		},
	})

	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestTimeout(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.PublishJSON(PublishJSONOptions{
		Body:    map[string]any{"ex_key": "ex_value"},
		Url:     "https://example.com",
		Timeout: "1s",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)

	AssertDeliveredEventually(t, client, res.MessageId)
}

func AssertDeliveredEventually(t *testing.T, client *Client, messageId string) {
	assert.Eventually(t, func() bool {
		subT := &testing.T{}

		events, _, err := client.Events().List(ListEventsOptions{
			Filter: EventFilter{
				MessageId: messageId,
				State:     Delivered,
			},
		})

		assert.NoError(subT, err)
		assert.Len(subT, events, 1)

		return !subT.Failed()
	}, time.Second*30, 100*time.Millisecond)
}
