package testutils

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := StopSharedPostgres(); err != nil {
		fmt.Fprintf(os.Stderr, "terminate shared PostgreSQL test container: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
