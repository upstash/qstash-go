package qstash

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestKeysGet(t *testing.T) {
	client := NewClientWithEnv()

	keys, err := client.Keys().Get()
	assert.NoError(t, err)

	assert.Equal(t, keys.Current, os.Getenv("QSTASH_CURRENT_SIGNING_KEY"))
	assert.Equal(t, keys.Next, os.Getenv("QSTASH_NEXT_SIGNING_KEY"))
}
