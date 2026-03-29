package qstash

import (
	"fmt"
	"net/http"
)

// Schedules in QStash allow you to publish messages at specified intervals instead of just once.
// You can create schedules using cron expressions.
// These expressions define the timing of message delivery, evaluated in the UTC timezone.
type Schedules struct {
	client *Client
}

type Schedule struct {
	// Id is the unique id of the schedule.
	Id string `json:"scheduleId"`
	// CreatedAt is the creation time of the schedule, in unix milliseconds.
	CreatedAt int64 `json:"createdAt"`
	// Cron is the cron expression used to schedule the messages.
	Cron string `json:"cron"`
	// Destination is the destination url or url group.
	Destination string `json:"destination"`
	// Method is the HTTP method to use for the message.
	Method string `json:"method"`
	// Header is the headers of the message.
	Header http.Header `json:"header,omitempty"`
	// Body is the body of the scheduled message if it is composed of UTF-8 characters only, empty otherwise.
	Body string `json:"body,omitempty"`
	// BodyBase64 is he base64 encoded body if the scheduled message body contains non-UTF-8 characters, empty otherwise.
	BodyBase64 string `json:"bodyBase64,omitempty"`
	// Retries is the number of retries that should be attempted in case of delivery failure.
	Retries int32 `json:"retries"`
	// Delay is the delay in seconds before the message is delivered.
	Delay int32 `json:"delay,omitempty"`
	// Callback is the url which is called each time the message is attempted to be delivered.
	Callback string `json:"callback,omitempty"`
	// FailureCallback is the url which is called after the message is failed
	FailureCallback string `json:"failureCallback,omitempty"`
	// LastScheduleTime is the timestamp in unix milli of last scheduled message
	LastScheduleTime int64 `json:"lastScheduleTime,omitempty"`
	// LastScheduleTime is the timestamp in unix milli of the next scheduled message
	NextScheduleTime int64 `json:"nextScheduleTime,omitempty"`
	// LastScheduleStates is the message id state pair for last schedule.
	LastScheduleStates map[string]string `json:"lastScheduleStates,omitempty"`
	// CallerIP is IP address of the creator of this schedule.
	CallerIP string `json:"callerIP,omitempty"`
	// IsPaused indicates whether the schedule is paused or not.
	IsPaused bool `json:"isPaused,omitempty"`
}

func (s *Schedules) Create(destination string, cron string, o ...ScheduleOption) (string, error) {
	req := request{
		method: http.MethodPost,
		path:   fmt.Sprintf("/v2/schedules/%s", destination),
	}
	for _, opt := range o {
		err := opt(&req)
		if err != nil {
			return "", err
		}
	}
	// Set cron header manually since it is a required field, not optional.
	req.header.Set(upstashCronHeader, cron)
	response, _, err := s.client.fetchWith(req)
	if err != nil {
		return "", err
	}
	result, err := parse[scheduleResponse](response)
	if err != nil {
		return "", err
	}
	return result.ScheduleId, err
}

type scheduleResponse struct {
	ScheduleId string `json:"scheduleId"`
}

// Get retrieves the schedule by its id.
func (s *Schedules) Get(scheduleId string) (Schedule, error) {
	opts := request{
		method: http.MethodGet,
		path:   fmt.Sprintf("/v2/schedules/%s", scheduleId),
	}
	response, _, err := s.client.fetchWith(opts)
	if err != nil {
		return Schedule{}, err
	}
	return parse[Schedule](response)
}

// List retrieves all the schedules.
func (s *Schedules) List() ([]Schedule, error) {
	opts := request{
		method: http.MethodGet,
		path:   "/v2/schedules",
	}
	response, _, err := s.client.fetchWith(opts)
	if err != nil {
		return nil, err
	}
	return parse[[]Schedule](response)
}

// Pause pauses the schedule.
// A paused schedule will not trigger new messages until it is resumed, even if the cron hits.
func (s *Schedules) Pause(scheduleId string) error {
	opts := request{
		method: http.MethodPatch,
		path:   fmt.Sprintf("/v2/schedules/%s/pause", scheduleId),
	}
	_, _, err := s.client.fetchWith(opts)
	return err
}

// Resume resumes the schedule.
func (s *Schedules) Resume(scheduleId string) error {
	opts := request{
		method: http.MethodPatch,
		path:   fmt.Sprintf("/v2/schedules/%s/resume", scheduleId),
	}
	_, _, err := s.client.fetchWith(opts)
	return err
}

// Delete deletes the schedule.
func (s *Schedules) Delete(scheduleId string) error {
	opts := request{
		method: http.MethodDelete,
		path:   fmt.Sprintf("/v2/schedules/%s", scheduleId),
	}
	_, _, err := s.client.fetchWith(opts)
	return err
}
