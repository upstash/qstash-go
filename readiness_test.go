package qstash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadiness(t *testing.T) {
	client := NewClientWithEnv()

	readiness, err := client.Readiness().Check()
	assert.NoError(t, err)
	assert.True(t, readiness.Ready)
}
