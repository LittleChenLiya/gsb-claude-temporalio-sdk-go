package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStartWorkflowOptionsIdempotencyKeyExported verifies that the client-side
// idempotency key is exposed on the public StartWorkflowOptions.
func TestStartWorkflowOptionsIdempotencyKeyExported(t *testing.T) {
	opts := StartWorkflowOptions{
		ID:             "wf-id",
		TaskQueue:      "tq",
		IdempotencyKey: "my-idempotency-key",
	}
	require.Equal(t, "my-idempotency-key", opts.IdempotencyKey)
}
