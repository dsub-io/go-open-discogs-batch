package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMainHandlesSuccessAndFailure(t *testing.T) {
	originalExecute, originalExit := execute, exit
	t.Cleanup(func() {
		execute = originalExecute
		exit = originalExit
	})

	exitCode := 0
	execute = func() error { return nil }
	exit = func(code int) { exitCode = code }
	main()
	require.Zero(t, exitCode)

	execute = func() error { return errors.New("fixture") }
	main()
	require.Equal(t, 1, exitCode)
}
