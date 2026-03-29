package qstash

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchedule(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	// Create a schedule
	scheduleId, err := client.Schedules.Create("https://example.com", "1 1 1 1 1",
		client.Schedules.WithJSONBody(map[string]any{
			"ex_key": "ex_value",
		}),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, scheduleId)

	// Get a schedule
	schedule, err := client.Schedules.Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.Equal(t, schedule.Cron, "1 1 1 1 1")
	assert.Equal(t, schedule.Destination, "https://example.com")

	// List all schedules
	schedules, err := client.Schedules.List()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(schedules), 1)

	scheduleIds := make([]string, len(schedules))
	for idx, s := range schedules {
		scheduleIds[idx] = s.Id
	}
	assert.Contains(t, scheduleIds, scheduleId)

	// Delete the schedule
	err = client.Schedules.Delete(scheduleId)
	assert.NoError(t, err)

	schedules, err = client.Schedules.List()
	assert.NoError(t, err)
	scheduleIds = make([]string, len(schedules))
	for idx, s := range schedules {
		scheduleIds[idx] = s.Id
	}
	assert.NotContains(t, schedules, scheduleId)
}

func TestSchedulePauseAndResume(t *testing.T) {
	client, err := NewClientWithEnv()
	assert.NoError(t, err)

	body, err := json.Marshal(map[string]any{
		"ex_key": "ex_value",
	})
	assert.NoError(t, err)

	// Create a schedule
	scheduleId, err := client.Schedules.Create("https://example.com", "1 1 1 1 1",
		client.Schedules.WithBody(bytes.NewReader(body)),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, scheduleId)

	// Get a schedule
	schedule, err := client.Schedules.Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.Equal(t, schedule.Cron, "1 1 1 1 1")
	assert.Equal(t, schedule.Destination, "https://example.com")
	assert.Equal(t, schedule.Body, body)
	assert.False(t, schedule.IsPaused)

	// Pause the schedule
	err = client.Schedules.Pause(scheduleId)
	assert.NoError(t, err)

	schedule, err = client.Schedules.Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.True(t, schedule.IsPaused)

	// Resume the schedule
	err = client.Schedules.Resume(scheduleId)
	assert.NoError(t, err)

	schedule, err = client.Schedules.Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.False(t, schedule.IsPaused)

	err = client.Schedules.Delete(scheduleId)
	assert.NoError(t, err)
}
