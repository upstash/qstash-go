package qstash

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidSignature = fmt.Errorf("failed to validate signature")
)

// Receiver offers a simple way to verify the signature of a request.
type Receiver struct {
	CurrentSigningKey string
	NextSigningKey    string
}

func NewReceiverWithEnv() *Receiver {
	return &Receiver{
		CurrentSigningKey: os.Getenv(currentSigningKeyEnvProperty),
		NextSigningKey:    os.Getenv(nextSigningKeyEnvProperty),
	}
}

func NewReceiver(currentSigningKey, nextSigningKey string) *Receiver {
	return &Receiver{
		CurrentSigningKey: currentSigningKey,
		NextSigningKey:    nextSigningKey,
	}
}

type MultiRegionReceiver struct {
	EuCentral1 SigningKeys
	UsEast1    SigningKeys
}

func NewMultiRegionReceiver() *Receiver {
	return nil
}

type claims struct {
	Body string `json:"body"`
	jwt.RegisteredClaims
}

func verify(key string, opts VerifyOptions) (err error) {
	token, err := jwt.ParseWithClaims(opts.Signature, &claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSignature
		}
		return []byte(key), nil
	}, jwt.WithLeeway(opts.Tolerance), jwt.WithIssuer("Upstash"))
	if err != nil {
		return ErrInvalidSignature
	}
	c, ok := token.Claims.(*claims)
	if !ok {
		return ErrInvalidSignature
	}
	if opts.Url != "" && c.Subject != opts.Url {
		return ErrInvalidSignature
	}
	h := sha256.New()
	if opts.BodyReader != nil {
		_, err = io.Copy(h, opts.BodyReader)
		if err != nil {
			return ErrInvalidSignature
		}
	} else if opts.Body != nil {
		_, err = h.Write(opts.Body)
		if err != nil {
			return ErrInvalidSignature
		}
	}
	bHash := h.Sum(nil)
	b64hash := strings.Trim(base64.URLEncoding.EncodeToString(bHash), "=")
	if strings.Trim(c.Body, "=") != b64hash {
		return ErrInvalidSignature
	}
	return nil
}

type VerifyOptions struct {
	// Signature is the signature from the `Upstash-Signature` header.
	Signature string
	// Url is the address of the endpoint where the request was sent to. When set to `None`, url is not check.
	Url string
	// Body is the raw request body.
	BodyReader io.Reader
	Body       []byte
	// Tolerance is the duration to tolerate when checking `nbf` and `exp` claims, to deal with small clock differences among different servers.
	Tolerance time.Duration
	Region    string
}

// Verify verifies the signature of a request.
// It tries to verify the signature with the current signing key.
// If that fails, maybe because you have rotated the keys recently, it will try to verify the signature with the next signing key.
func (r *Receiver) Verify(opts VerifyOptions) (err error) {
	err = verify(r.CurrentSigningKey, opts)
	if errors.Is(err, ErrInvalidSignature) {
		err = verify(r.NextSigningKey, opts)
	}
	return
}
