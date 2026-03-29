package qstash

type queueOptions struct {
	QueueName   string `json:"queueName"`
	Parallelism int    `json:"parallelism,omitempty"`
	Paused      bool   `json:"paused,omitempty"`
}
type QueueOption func(*queueOptions)

func (q *Queues) WithParallelism(parallelism int) QueueOption {
	return func(q *queueOptions) {
		q.Parallelism = parallelism
	}
}

func (q *Queues) WithPaused(paused bool) QueueOption {
	return func(q *queueOptions) {
		q.Paused = paused
	}
}
