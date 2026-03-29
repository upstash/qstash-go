package qstash

import (
	"net/http"
)

type Logs struct {
	client *Client
}

type LogState string

var (
	Created         LogState = "CREATED"
	Active          LogState = "ACTIVE"
	Retry           LogState = "RETRY"
	Error           LogState = "ERROR"
	Delivered       LogState = "DELIVERED"
	Failed          LogState = "FAILED"
	CancelRequested LogState = "CANCEL_REQUESTED"
	Canceled        LogState = "CANCELED"
)

type Log struct {
	// Time is the timestamp of this event in Unix time (milliseconds).
	Time int64 `json:"time"`
	// MessageId is the ID of associated message.
	MessageId string `json:"messageId"`
	// State is the current state of the message.
	State LogState `json:"state"`
	// Error is set only if the status of the message is an error.
	Error string `json:"error,omitempty"`
	// NextDeliveryTime is the next scheduled time of the message in milliseconds
	NextDeliveryTime int64 `json:"nextDeliveryTime,omitempty"`
	// Url is the destination url
	Url string `json:"url"`
	// UrlGroup is the name of the url group if this message was sent through an url group, empty otherwise.
	UrlGroup string `json:"topicName,omitempty"`
	// Endpoint is the name of the endpoint if this message was sent through an url group, empty otherwise.
	EndpointName string `json:"endpointName,omitempty"`
	// Api is the name of the api if this message was sent to an api.
	Api string `json:"api,omitempty"`
	// QueueName is the name of the queue if this message is enqueued on a queue, empty otherwise.
	QueueName string `json:"queueName,omitempty"`
	// ScheduleId is the ID of responsible schedule if the message is triggered by a schedule.
	ScheduleId string `json:"scheduleId,omitempty"`
}

type listEventsResponse struct {
	Cursor string `json:"cursor,omitempty"`
	Events []Log  `json:"events"`
}

// List retrieves all events that occurred, such as message creation or delivery.
func (l *Logs) List(o ...LogsOptions) ([]Log, string, error) {
	opts := request{
		method: http.MethodGet,
		path:   "/v2/events",
	}
	for _, opt := range o {
		err := opt(&opts)
		if err != nil {
			return nil, "", err
		}
	}
	response, _, err := l.client.fetchWith(opts)
	if err != nil {
		return nil, "", err
	}
	events, err := parse[listEventsResponse](response)
	if err != nil {
		return nil, "", err
	}
	return events.Events, events.Cursor, nil
}
