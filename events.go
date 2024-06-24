package qstash

import (
	"net/http"
	"time"
)

type Events struct {
	client *Client
}

type EventState string

var (
	Created         EventState = "CREATED"
	Active          EventState = "ACTIVE"
	Retry           EventState = "RETRY"
	Error           EventState = "ERROR"
	Delivered       EventState = "DELIVERED"
	Failed          EventState = "FAILED"
	CancelRequested EventState = "CANCEL_REQUESTED"
	Canceled        EventState = "CANCELED"
)

type Event struct {
	// Time is the timestamp of this event in Unix time (milliseconds).
	Time int64 `json:"time"`
	// MessageId is the ID of associated message.
	MessageId string `json:"messageId"`
	// State is the current state of the message.
	State EventState `json:"state"`
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

type EventFilter struct {
	// MessageId filters events by the ID of the message.
	MessageId string
	// State filters events by the state of the message.
	State EventState
	// Url filters events by the URL of the message.
	Url string
	// UrlGroup filters events by URL group of the message.
	UrlGroup string
	// Api filters events by the API name of the message.
	Api string
	// Queue filters events by queue name.
	Queue string
	// ScheduleId filters events by schedule ID.
	ScheduleId string
	// FromDate filters events by starting time in milliseconds.
	FromDate time.Time
	// ToDate filters events by ending time in milliseconds.
	ToDate time.Time
}

type listEventsResponse struct {
	Cursor string  `json:"cursor,omitempty"`
	Events []Event `json:"events"`
}

// List retrieves all events that occurred, such as message creation or delivery.
func (e *Events) List(options ListEventsOptions) ([]Event, string, error) {
	opts := requestOptions{
		method: http.MethodGet,
		path:   "/v2/events",
		params: options.Params(),
	}
	response, _, err := e.client.fetchWith(opts)
	if err != nil {
		return nil, "", err
	}
	events, err := parse[listEventsResponse](response)
	if err != nil {
		return nil, "", err
	}
	return events.Events, events.Cursor, nil
}
