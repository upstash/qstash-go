package qstash

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

const (
	tokenEnvProperty             = "QSTASH_TOKEN"
	urlEnvProperty               = "QSTASH_URL"
	currentSigningKeyEnvProperty = "QSTASH_CURRENT_SIGNING_KEY"
	nextSigningKeyEnvProperty    = "QSTASH_NEXT_SIGNING_KEY"

	upstashMethodHeader              = "Upstash-Method"
	upstashRetriesHeader             = "Upstash-Retries"
	upstashCallbackHeader            = "Upstash-Callback"
	upstashFailureCallbackHeader     = "Upstash-Failure-Callback"
	upstashCronHeader                = "Upstash-Cron"
	upstashDelayHeader               = "Upstash-Delay"
	upstashTimeoutHeader             = "Upstash-Timeout"
	upstashDeduplicationId           = "Upstash-Deduplication-Id"
	upstashNotBefore                 = "Upstash-Not-Before"
	upstashContentBasedDeduplication = "Upstash-Content-Based-Deduplication"
	upstashScheduleId                = "Upstash-Schedule-Id"
	upstashRetryDelayHeader          = "Upstash-Retry-Delay"
	upstashLabelHeader               = "Upstash-Label"
)

type Options struct {
	// Url is the base address of QStash, it's set to https://qstash.upstash.io by default.
	Url string
	// Token is the authorization token from the Upstash console.
	Token string
	// Client is the HTTP client used for sending requests.
	Client *http.Client
}

func (o *Options) init() {
	if o.Url == "" {
		o.Url = "https://qstash.upstash.io"
	}
	if o.Client == nil {
		o.Client = http.DefaultClient
	}
	if o.Token == "" {
		panic("Missing QStash Token")
	}
}

// NewClient initializes a client instance with the given token and the default HTTP client.
func NewClient(token string) (*Client, error) {
	return NewClientWith(Options{
		Token: token,
	})
}

// NewClientWithEnv initializes a client with the token from the QSTASH_TOKEN environment variable and the default HTTP client.
func NewClientWithEnv() (*Client, error) {
	return NewClientWith(Options{
		Token: os.Getenv(tokenEnvProperty),
	})
}

// NewClientWith initializes a client with the given token and HTTP client.
func NewClientWith(options Options) (*Client, error) {
	options.init()
	header := http.Header{}
	header.Set("Authorization", "Bearer "+options.Token)
	base := os.Getenv(urlEnvProperty)
	if base == "" {
		base = options.Url
	}
	client := &Client{
		token:   options.Token,
		client:  options.Client,
		url:     base,
		headers: header,
	}
	client.Publish = func(destination string, o ...PublishOption) (MessageReceipt, error) {
		return publish(client, destination, o...)
	}
	client.Enqueue = func(url string, queueName string, o ...EnqueueOption) (MessageReceipt, error) {
		return enqueue(client, url, queueName, o...)
	}
	client.Batch = func(options [][]BatchOption) (results [][]MessageReceipt, err error) {
		return batch(client, options)
	}
	return client, nil
}

type Client struct {
	token   string
	client  *http.Client
	url     string
	headers http.Header

	Schedules *Schedules
	Dlq       *Dlq
	Logs      *Logs
	UrlGroups *UrlGroups
	Keys      *Keys
	Messages  *Messages
	Queues    *Queues

	Publish Publish
	Enqueue Enqueue
	Batch   Batch
}

type request struct {
	method      string
	path        string
	body        io.Reader
	header      http.Header
	params      url.Values
	contentType string
}

func (c *Client) fetchWith(opts request) ([]byte, int, error) {
	req, err := http.NewRequest(opts.method, fmt.Sprintf("%s%s", c.url, opts.path), opts.body)
	if err != nil {
		return nil, -1, err
	}
	if opts.params != nil {
		req.URL.RawQuery = opts.params.Encode()
	}
	hc := c.headers.Clone()
	for k, v := range opts.header {
		hc.Set(k, v[0])
	}
	req.Header = hc
	if opts.contentType != "" {
		req.Header.Set("Content-Type", opts.contentType)
	}
	res, err := c.client.Do(req)
	if err != nil {
		return nil, -1, err
	}
	defer res.Body.Close()
	response, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, -1, err
	}
	if res.StatusCode >= http.StatusBadRequest {
		var rErr restError
		if err = json.Unmarshal(response, &rErr); err != nil {
			return response, res.StatusCode, err
		}
		return response, res.StatusCode, errors.New(rErr.Error)
	}
	return response, res.StatusCode, nil
}

type restError struct {
	Error string `json:"error"`
}

func parse[T any](data []byte) (t T, err error) {
	err = json.Unmarshal(data, &t)
	return
}
