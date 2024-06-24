package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchedule(t *testing.T) {
	client := NewClientWithEnv()

	// Create a schedule
	scheduleId, err := client.Schedules().CreateJSON(ScheduleJSONOptions{
		Cron:        "1 1 1 1 1",
		Destination: "https://example.com",
		Body: map[string]any{
			"ex_key": "ex_value",
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, scheduleId)

	// Get a schedule
	schedule, err := client.Schedules().Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.Equal(t, schedule.Cron, "1 1 1 1 1")
	assert.Equal(t, schedule.Destination, "https://example.com")

	// List all schedules
	schedules, err := client.Schedules().List()
	assert.NoError(t, err)
	assert.Len(t, schedules, 1)
	assert.Equal(t, schedules[0].Id, scheduleId)

	// Delete the schedule
	err = client.Schedules().Delete(scheduleId)
	assert.NoError(t, err)

	schedules, err = client.Schedules().List()
	assert.NoError(t, err)
	assert.Empty(t, schedules)
}

func TestSchedulePauseAndResume(t *testing.T) {
	client := NewClientWithEnv()

	// Create a schedule
	scheduleId, err := client.Schedules().CreateJSON(ScheduleJSONOptions{
		Cron:        "1 1 1 1 1",
		Destination: "https://example.com",
		Body: map[string]any{
			"ex_key": "ex_value",
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, scheduleId)

	// Get a schedule
	schedule, err := client.Schedules().Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.Equal(t, schedule.Cron, "1 1 1 1 1")
	assert.Equal(t, schedule.Destination, "https://example.com")
	assert.False(t, schedule.IsPaused)

	// Pause the schedule
	err = client.Schedules().Pause(scheduleId)
	assert.NoError(t, err)

	schedule, err = client.Schedules().Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.True(t, schedule.IsPaused)

	// Resume the schedule
	err = client.Schedules().Resume(scheduleId)
	assert.NoError(t, err)

	schedule, err = client.Schedules().Get(scheduleId)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Id, scheduleId)
	assert.False(t, schedule.IsPaused)

	err = client.Schedules().Delete(scheduleId)
	assert.NoError(t, err)
}
