package database

import (
	"fmt"
	"os"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := testutils.StopSharedPostgres(); err != nil {
		fmt.Fprintf(os.Stderr, "terminate shared PostgreSQL test container: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
