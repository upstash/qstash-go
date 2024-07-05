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
		assert.NotEmpty(t, response)
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}

	}
}

func TestBatchMixed(t *testing.T) {
	client := NewClientWithEnv()

	name := "go_url_group"

	err := client.UrlGroups().Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups().UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com", Name: "First endpoint"},
		{Url: "https://example.net", Name: "Second endpoint"},
	})
	assert.NoError(t, err)

	urlGroup, err := client.UrlGroups().Get(name)
	assert.NoError(t, err)
	assert.Equal(t, urlGroup.Name, name)
	assert.Len(t, urlGroup.Endpoints, 2)

	N := 3
	messages := make([]BatchOptions, N)

	for i := 0; i < N; i++ {
		if i%2 == 0 {
			messages[i] = BatchOptions{
				UrlGroup: name,
				Body:     fmt.Sprintf("hi %d", i),
				Retries:  RetryCount(0),
				Headers: map[string]string{
					fmt.Sprintf("test-header-%d", i): fmt.Sprintf("test-value-%d", i),
					"Content-Type":                   "text/plain",
				},
			}
		} else {
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

	}

	responses, err := client.Batch(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}
	}

	err = client.UrlGroups().Delete(name)
	assert.NoError(t, err)
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
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}
	}
}

func TestBatchJSONMixed(t *testing.T) {
	client := NewClientWithEnv()

	name := "go_url_group"

	err := client.UrlGroups().Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups().UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com", Name: "First endpoint"},
		{Url: "https://example.net", Name: "Second endpoint"},
	})
	assert.NoError(t, err)

	urlGroup, err := client.UrlGroups().Get(name)
	assert.NoError(t, err)
	assert.Equal(t, urlGroup.Name, name)
	assert.Len(t, urlGroup.Endpoints, 2)

	N := 3
	messages := make([]BatchJSONOptions, N)

	for i := 0; i < N; i++ {
		if i%2 == 0 {
			messages[i] = BatchJSONOptions{
				UrlGroup: name,
				Body:     map[string]any{"hi": i},
				Retries:  RetryCount(0),
				Headers: map[string]string{
					fmt.Sprintf("test-header-%d", i): fmt.Sprintf("test-value-%d", i),
					"Content-Type":                   "text/plain",
				},
			}
		} else {
			messages[i] = BatchJSONOptions{
				Body:    map[string]any{"hi": i},
				Url:     "https://example.com",
				Retries: RetryCount(0),
				Headers: map[string]string{
					fmt.Sprintf("test-header-%d", i): fmt.Sprintf("test-value-%d", i),
					"Content-Type":                   "text/plain",
				},
			}
		}

	}

	responses, err := client.BatchJSON(messages)
	assert.NoError(t, err)
	assert.Len(t, responses, N)

	for _, response := range responses {
		for _, r := range response {
			assert.NotEmpty(t, r.MessageId)
		}
	}

	err = client.UrlGroups().Delete(name)
	assert.NoError(t, err)
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
	assert.Len(t, messages[0], 1)
	assert.NotEmpty(t, messages[0][0].MessageId)

	AssertDeliveredEventually(t, client, messages[0][0].MessageId)
}

func TestEnqueue(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.Enqueue(EnqueueOptions{
		Queue: "test-queue",
		Body:  "test-body",
		Url:   "https://example.com",
		Headers: map[string]string{
			"test-header": "test-value",
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestEnqueueJSON(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.EnqueueJSON(EnqueueJSONOptions{
		Queue: "test-queue",
		Body:  map[string]any{"test": "body"},
		Url:   "https://example.com",
		Headers: map[string]string{
			"test-header": "test-value",
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, res.MessageId)
}

func TestEnqueueLlmApi(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.EnqueueJSON(EnqueueJSONOptions{
		Queue: "test-queue",
		Api:   "llm",
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

func TestCancelMany(t *testing.T) {
	client := NewClientWithEnv()

	messageIds := []string{}

	for i := 0; i < 10; i++ {
		res, err := client.PublishJSON(PublishJSONOptions{
			Body:  map[string]any{"ex_key": "ex_value"},
			Url:   "https://example.com",
			Delay: "60s",
		})
		assert.NoError(t, err)
		if i%2 == 0 {
			assert.NotEmpty(t, res.MessageId)
			messageIds = append(messageIds, res.MessageId)
		}
	}
	deleted, err := client.Messages().CancelMany(messageIds)
	assert.NoError(t, err)
	assert.Equal(t, 5, deleted)
}

func TestCancelAll(t *testing.T) {
	client := NewClientWithEnv()

	for i := 0; i < 10; i++ {
		res, err := client.PublishJSON(PublishJSONOptions{
			Body:    map[string]any{"ex_key": "ex_value"},
			Url:     "http://httpstat.us/400",
			Delay:   "30s",
			Retries: RetryCount(0),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, res.MessageId)
	}

	time.Sleep(1 * time.Second)
	deleted, err := client.Messages().CancelAll()
	assert.NoError(t, err)
	assert.Greater(t, deleted, 0)
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
