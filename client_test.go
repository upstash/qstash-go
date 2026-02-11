package qstash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadiness(t *testing.T) {
	client := NewClientWithEnv()

	res, err := client.Readiness()
	assert.NoError(t, err)
	assert.True(t, res.Success)
	assert.GreaterOrEqual(t, res.StatusCode, 200)
	assert.Less(t, res.StatusCode, 300)
}
