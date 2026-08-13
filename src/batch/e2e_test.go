//go:build e2e

package batch

import "testing"

// TestBatchE2E runs the complete fixture-to-PostgreSQL scenario only in the
// separately required E2E lane.
func TestBatchE2E(t *testing.T) {
	runBatchE2E(t)
}
