package qstash

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"
)

func sign(body string, key string) (string, error) {
	// Compute SHA-256 hash
	hash := sha256.New()
	hash.Write([]byte(body))
	bodyHash := hash.Sum(nil)

	bodyHashBase64 := base64.URLEncoding.EncodeToString(bodyHash)
	bodyHashBase64 = strings.Trim(bodyHashBase64, "=")

	// Create JWT payload
	now := time.Now().Unix()
	payload := jwt.MapClaims{
		"aud":  "",
		"body": bodyHashBase64,
		"exp":  now + 300,
		"iat":  now,
		"iss":  "Upstash",
		"jti":  fmt.Sprintf("%f", float64(now)), // Converting time to a string to mimic Python's time.time()
		"nbf":  now,
		"sub":  "https://example.com",
	}

	// Create JWT token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, payload)
	token.Header["alg"] = "HS256"
	token.Header["typ"] = "JWT"

	// Sign the token
	signature, err := token.SignedString([]byte(key))
	if err != nil {
		return "", err
	}

	return signature, nil
}

func TestVerify(t *testing.T) {

	receiver := NewReceiverWithEnv()

	body, err := json.Marshal(map[string]string{"hello": "world"})
	assert.NoError(t, err)

	signature, err := sign(string(body), os.Getenv(currentSigningKeyEnvProperty))
	assert.NoError(t, err)

	err = receiver.Verify(VerifyOptions{
		Signature: signature,
		Url:       "https://example.com",
		Body:      string(body),
	})
	assert.NoError(t, err)
}

func TestFailedVerify(t *testing.T) {
	receiver := NewReceiverWithEnv()

	body, err := json.Marshal(map[string]string{"hello": "world"})
	assert.NoError(t, err)

	signature, err := sign(string(body), os.Getenv(currentSigningKeyEnvProperty))
	assert.NoError(t, err)

	err = receiver.Verify(VerifyOptions{
		Signature: signature,
		Url:       "https://example.com/fail",
		Body:      string(body),
	})
	assert.Error(t, err)
}
