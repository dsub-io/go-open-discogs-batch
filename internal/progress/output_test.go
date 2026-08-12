package progress

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStructuredOutputSelection(t *testing.T) {
	var output bytes.Buffer

	_, err := structuredOutput(&output, false).Write([]byte("record"))
	require.NoError(t, err)
	require.Equal(t, "record", output.String())

	_, err = structuredOutput(&output, true).Write([]byte("hidden"))
	require.NoError(t, err)
	require.Equal(t, "record", output.String())

	_, err = structuredOutput(nil, false).Write([]byte("discarded"))
	require.NoError(t, err)
}

func TestStructuredOutputUsesNonTerminalFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "structured-output")
	require.NoError(t, err)

	output := StructuredOutput(file)
	_, err = output.Write([]byte("record"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	payload, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	require.Equal(t, "record", string(payload))
	require.NotEqual(t, io.Discard, output)
}
