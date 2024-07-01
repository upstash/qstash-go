package qstash

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	upstashForwardHeader             = "Upstash-Forward"
	upstashCronHeader                = "Upstash-Cron"
	upstashDelayHeader               = "Upstash-Delay"
	upstashTimeoutHeader             = "Upstash-Timeout"
	upstashDeduplicationId           = "Upstash-Deduplication-Id"
	upstashNotBefore                 = "Upstash-Not-Before"
	upstashContentBasedDeduplication = "Upstash-Content-Based-Deduplication"
)

var (
	contentTypeJson = http.Header{"Content-Type": []string{"application/json"}}
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
func NewClient(token string) *Client {
	return NewClientWith(Options{
		Token: token,
	})
}

// NewClientWithEnv initializes a client with the token from the QSTASH_TOKEN environment variable and the default HTTP client.
func NewClientWithEnv() *Client {
	return NewClientWith(Options{
		Token: os.Getenv(tokenEnvProperty),
	})
}

// NewClientWith initializes a client with the given token and HTTP client.
func NewClientWith(options Options) *Client {
	options.init()
	header := http.Header{}
	header.Set("Authorization", "Bearer "+options.Token)
	base := os.Getenv(urlEnvProperty)
	if base == "" {
		base = options.Url
	}
	index := &Client{
		token:   options.Token,
		client:  options.Client,
		url:     base,
		headers: header,
	}

	return index
}

type Client struct {
	token   string
	client  *http.Client
	url     string
	headers http.Header
}

func (c *Client) Schedules() *Schedules {
	return &Schedules{client: c}
}

func (c *Client) Dlq() *Dlq {
	return &Dlq{client: c}
}

func (c *Client) Events() *Events {
	return &Events{client: c}
}

func (c *Client) UrlGroups() *UrlGroups {
	return &UrlGroups{client: c}
}

func (c *Client) Keys() *Keys {
	return &Keys{client: c}
}

func (c *Client) Messages() *Messages {
	return &Messages{client: c}
}

func (c *Client) Queues() *Queues {
	return &Queues{client: c}
}

type requestOptions struct {
	method string
	path   string
	body   string
	header http.Header
	params url.Values
}

func (c *Client) fetchWith(opts requestOptions) ([]byte, int, error) {
	request, err := http.NewRequest(opts.method, fmt.Sprintf("%s%s", c.url, opts.path), strings.NewReader(opts.body))
	if err != nil {
		return nil, -1, err
	}
	if opts.params != nil {
		request.URL.RawQuery = opts.params.Encode()
	}
	hc := c.headers.Clone()
	for k, v := range opts.header {
		hc.Set(k, v[0])
	}
	request.Header = hc
	res, err := c.client.Do(request)
	if err != nil {
		return nil, -1, err
	}
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

func getDestination(url string, urlGroup string, api string) (string, error) {
	destination := ""
	count := 0
	if url != "" {
		destination = url
		count++
	}
	if urlGroup != "" {
		destination = urlGroup
		count++
	}
	if api != "" {
		destination = fmt.Sprintf("api/%s", api)
		count++
	}
	if count != 1 {
		return "", fmt.Errorf("multiple destinations found, configure only one of Url, UrlGroup or Api")
	}
	return destination, nil
}

func prepareHeaders(
	contentType string,
	method string,
	headers map[string]string,
	retries *int,
	callback string,
	failureCallback string,
	delay string,
	notBefore string,
	deduplicationId string,
	contentBasedDeduplication bool,
	timeout string,
	cron string,
) http.Header {
	header := http.Header{}
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	if method != "" {
		header.Set(upstashMethodHeader, method)
	}
	for k, v := range headers {
		if !strings.HasPrefix(strings.ToLower(k), "upstash-forward-") {
			k = fmt.Sprintf("%s-%s", upstashForwardHeader, k)
		}
		header.Set(k, v)
	}
	if retries != nil {
		header.Set(upstashRetriesHeader, fmt.Sprintf("%d", *retries))
	}
	if callback != "" {
		header.Set(upstashCallbackHeader, callback)
	}
	if failureCallback != "" {
		header.Set(upstashFailureCallbackHeader, failureCallback)
	}
	if delay != "" {
		header.Set(upstashDelayHeader, delay)
	}
	if notBefore != "" {
		header.Set(upstashNotBefore, notBefore)
	}
	if deduplicationId != "" {
		header.Set(upstashDeduplicationId, deduplicationId)
	}
	if contentBasedDeduplication {
		header.Set(upstashContentBasedDeduplication, "true")
	}
	if timeout != "" {
		header.Set(upstashTimeoutHeader, timeout)
	}
	if cron != "" {
		header.Set(upstashCronHeader, cron)
	}
	return header
}
