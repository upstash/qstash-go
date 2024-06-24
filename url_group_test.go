package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUrlGroup(t *testing.T) {
	now := time.Now()
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
	assert.Greater(t, urlGroup.CreatedAt, now.UnixMilli())
	assert.Greater(t, urlGroup.UpdatedAt, now.UnixMilli())
	assert.Equal(t, urlGroup.Endpoints[0], Endpoint{Url: "https://example.com", Name: "First endpoint"})
	assert.Equal(t, urlGroup.Endpoints[1], Endpoint{Url: "https://example.net", Name: "Second endpoint"})

	urlGroups, err := client.UrlGroups().List()
	assert.NoError(t, err)
	assert.Len(t, urlGroups, 1)
	assert.Equal(t, urlGroups[0].Name, name)

	err = client.UrlGroups().RemoveEndpoints(name, []Endpoint{
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	urlGroup, err = client.UrlGroups().Get(name)
	assert.NoError(t, err)
	assert.Equal(t, urlGroup.Name, name)
	assert.Len(t, urlGroup.Endpoints, 1)
	assert.Equal(t, urlGroup.Endpoints[0], Endpoint{Url: "https://example.com", Name: "First endpoint"})

	err = client.UrlGroups().Delete(name)
	assert.NoError(t, err)
}

func TestPublishToUrlGroup(t *testing.T) {
	client := NewClientWithEnv()

	name := "go_url_group"
	err := client.UrlGroups().Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups().UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com"},
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	res, err := client.UrlGroups().Publish(PublishOptions{
		UrlGroup: name,
		Body:     "test-body",
	})
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.NotEmpty(t, res[0].MessageId)
	assert.NotEmpty(t, res[1].MessageId)
}

func TestEnqueueToUrlGroup(t *testing.T) {
	client := NewClientWithEnv()

	name := "go_url_group"
	err := client.UrlGroups().Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups().UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com"},
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	res, err := client.UrlGroups().EnqueueJSON(EnqueueJSONOptions{
		Queue: "test-queue",
		PublishJSONOptions: PublishJSONOptions{
			UrlGroup: name,
			Body:     map[string]any{"test": "body"},
			Headers: map[string]string{
				"test-header": "test-value",
			},
		},
	})
	assert.NoError(t, err)

	assert.Len(t, res, 2)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.NotEmpty(t, res[0].MessageId)
	assert.NotEmpty(t, res[1].MessageId)
}
