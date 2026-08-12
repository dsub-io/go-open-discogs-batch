package progress

import (
	"io"
	"os"

	"golang.org/x/term"
)

// StructuredOutput suppresses machine-readable progress on an interactive stream.
func StructuredOutput(output *os.File) io.Writer {
	return structuredOutput(output, term.IsTerminal(int(output.Fd())))
}

func structuredOutput(output io.Writer, terminal bool) io.Writer {
	if output == nil || terminal {
		return io.Discard
	}
	return output
}
