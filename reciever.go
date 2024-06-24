package qstash

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"os"
	"strings"
	"time"
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
	h.Write([]byte(opts.Body))
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
	Body string
	// Tolerance is the duration to tolerate when checking `nbf` and `exp` claims, to deal with small clock differences among different servers.
	Tolerance time.Duration
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
