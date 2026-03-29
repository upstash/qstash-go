package qstash

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUrlGroup(t *testing.T) {
	now := time.Now()
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
	assert.Greater(t, urlGroup.CreatedAt, now.UnixMilli())
	assert.Greater(t, urlGroup.UpdatedAt, now.UnixMilli())
	assert.Equal(t, urlGroup.Endpoints[0], Endpoint{Url: "https://example.com", Name: "First endpoint"})
	assert.Equal(t, urlGroup.Endpoints[1], Endpoint{Url: "https://example.net", Name: "Second endpoint"})

	urlGroups, err := client.UrlGroups.List()
	assert.NoError(t, err)
	assert.Len(t, urlGroups, 1)
	assert.Equal(t, urlGroups[0].Name, name)

	err = client.UrlGroups.RemoveEndpoints(name, []Endpoint{
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	urlGroup, err = client.UrlGroups.Get(name)
	assert.NoError(t, err)
	assert.Equal(t, urlGroup.Name, name)
	assert.Len(t, urlGroup.Endpoints, 1)
	assert.Equal(t, urlGroup.Endpoints[0], Endpoint{Url: "https://example.com", Name: "First endpoint"})

	err = client.UrlGroups.Delete(name)
	assert.NoError(t, err)
}

func TestPublishToUrlGroup(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	name := "go_url_group"
	err = client.UrlGroups.Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups.UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com"},
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	res, err := client.UrlGroups.Publish(name,
		client.Publish.WithBody(strings.NewReader("test-body")),
	)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.NotEmpty(t, res[0].MessageId)
	assert.NotEmpty(t, res[1].MessageId)
}

func TestUrlGroup_Error(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	name := "non_existing_url_group"

	_, err = client.UrlGroups.Publish(name, client.Publish.WithBody(strings.NewReader("test-body")))
	assert.ErrorContains(t, err, "topic non_existing_url_group not found")
}

func TestUrlGroup_Empty(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	_, err = client.UrlGroups.Publish("")

	assert.ErrorContains(t, err, "a non-empty destination must be provided")
}

func TestEnqueueToUrlGroup(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	name := "go_url_group"
	err = client.UrlGroups.Delete(name)
	assert.NoError(t, err)

	err = client.UrlGroups.UpsertEndpoints(name, []Endpoint{
		{Url: "https://example.com"},
		{Url: "https://example.net"},
	})
	assert.NoError(t, err)

	res, err := client.UrlGroups.Enqueue(name, "test-queue",
		client.Enqueue.WithBody(strings.NewReader("test-body")),
		client.Enqueue.WithHeader(http.Header{"test-header": []string{"test-value"}}),
	)
	assert.NoError(t, err)

	assert.Len(t, res, 2)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.NotEmpty(t, res[0].MessageId)
	assert.NotEmpty(t, res[1].MessageId)
}
