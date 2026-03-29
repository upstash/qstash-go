package qstash

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.ngrok.com/ngrok"
	"golang.ngrok.com/ngrok/config"
)

func ServeTestEndpoint(t *testing.T, handler http.Handler) string {
	t.Helper()

	listener, err := ngrok.Listen(
		context.Background(),
		config.HTTPEndpoint(),
		ngrok.WithAuthtokenFromEnv(),
	)
	if err != nil {
		t.Fatalf("failed to start ngrok tunnel: %v", err)
	}

	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
	})

	go func() {
		// server closed is expected on cleanup, ignore it
		_ = http.Serve(listener, handler)
	}()

	return listener.URL()
}
