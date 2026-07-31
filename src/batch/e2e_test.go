//go:build e2e

package batch

import "testing"

// TestBatchE2E reuses the complete fixture-to-PostgreSQL scenario in the
// separately required E2E lane. TestBatch remains in the regular test suite.
func TestBatchE2E(t *testing.T) {
	TestBatch(t)
}
