package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueue(t *testing.T) {
	client := NewClientWithEnv()

	name := "test-queue"
	err := client.Queues().Upsert(Queue{
		Name:        name,
		Parallelism: 1,
	})
	assert.NoError(t, err)

	queue, err := client.Queues().Get(name)
	assert.NoError(t, err)
	assert.Equal(t, queue.Name, name)
	assert.Equal(t, queue.Parallelism, 1)

	// Reconfigure queue parallelism
	err = client.Queues().Upsert(Queue{
		Name:        name,
		Parallelism: 2,
	})
	assert.NoError(t, err)

	queues, err := client.Queues().List()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	assert.Equal(t, queues[0].Name, name)
	assert.Equal(t, queues[0].Parallelism, 2)

	// Delete queue
	err = client.Queues().Delete(name)
	assert.NoError(t, err)

	queues, err = client.Queues().List()
	assert.NoError(t, err)
	assert.Empty(t, queues)
}

func TestQueuePauseAndResume(t *testing.T) {
	client := NewClientWithEnv()

	name := "test-queue"
	err := client.Queues().Upsert(Queue{
		Name:        name,
		Parallelism: 1,
	})
	assert.NoError(t, err)

	queue, err := client.Queues().Get(name)
	assert.NoError(t, err)
	assert.False(t, queue.IsPaused)

	// Pause the queue
	err = client.Queues().Pause(name)
	assert.NoError(t, err)

	queue, err = client.Queues().Get(name)
	assert.NoError(t, err)
	assert.True(t, queue.IsPaused)

	// Resume the queue
	err = client.Queues().Resume(name)
	assert.NoError(t, err)

	queue, err = client.Queues().Get(name)
	assert.NoError(t, err)
	assert.False(t, queue.IsPaused)

	// Pause the queue with upsert
	err = client.Queues().Upsert(Queue{
		Name:        name,
		Parallelism: 1,
		IsPaused:    true,
	})
	assert.NoError(t, err)

	queue, err = client.Queues().Get(name)
	assert.NoError(t, err)
	assert.True(t, queue.IsPaused)

	// Resume the queue with upsert
	err = client.Queues().Upsert(Queue{
		Name:        name,
		Parallelism: 1,
		IsPaused:    false,
	})
	assert.NoError(t, err)

	queue, err = client.Queues().Get(name)
	assert.NoError(t, err)
	assert.False(t, queue.IsPaused)
}
