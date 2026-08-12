package progress

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReporterLifecycle(t *testing.T) {
	var output bytes.Buffer
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	reporter := newReporter(
		&output,
		io.Discard,
		false,
		StageDownload,
		"dump.xml.gz",
		100,
		defaultReportInterval,
		func() time.Time { return now },
	)

	reporter.Start()
	reporter.Start()
	reporter.Add(0)
	reporter.Add(-1)
	now = now.Add(time.Second)
	reporter.Add(25)
	reporter.Set(10)
	now = now.Add(4 * time.Second)
	reporter.Set(50)
	reporter.Set(150)
	reporter.Set(100)
	reporter.Fail(100)

	records := decodeRecords(t, output.Bytes())
	require.Len(t, records, 3)
	require.Equal(t, Record{
		Event:          EventByteProgress,
		State:          stateStarted,
		Stage:          StageDownload,
		Resource:       "dump.xml.gz",
		CompletedBytes: 0,
		TotalBytes:     100,
		Percent:        0,
		BytesPerSecond: 0,
		ElapsedSeconds: 0,
	}, records[0])
	require.Equal(t, stateRunning, records[1].State)
	require.Equal(t, int64(50), records[1].CompletedBytes)
	require.Equal(t, 50.0, records[1].Percent)
	require.Equal(t, 10.0, records[1].BytesPerSecond)
	require.Equal(t, stateCompleted, records[2].State)
	require.Equal(t, int64(100), records[2].CompletedBytes)
	require.Equal(t, 100.0, records[2].Percent)
	require.Equal(t, 20.0, records[2].BytesPerSecond)
	require.NotContains(t, output.String(), "\x1b")
	require.NotContains(t, output.String(), "\r")
}

func TestReporterStartsImplicitlyAndFails(t *testing.T) {
	var output bytes.Buffer
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	reporter := newReporter(
		&output,
		io.Discard,
		false,
		StageSourceRead,
		"artists",
		100,
		defaultReportInterval,
		func() time.Time { return now },
	)

	reporter.Set(10)
	now = now.Add(time.Second)
	reporter.Fail(200)
	reporter.Fail(50)

	records := decodeRecords(t, output.Bytes())
	require.Len(t, records, 2)
	require.Equal(t, stateStarted, records[0].State)
	require.Equal(t, stateFailed, records[1].State)
	require.Equal(t, int64(100), records[1].CompletedBytes)
	require.Equal(t, 100.0, records[1].Percent)
}

func TestReporterSupportsUnknownTotalsAndDiscardedOutput(t *testing.T) {
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	reporter := newReporter(
		nil,
		nil,
		false,
		StageDownload,
		"unknown",
		-1,
		defaultReportInterval,
		func() time.Time { return now },
	)

	reporter.Start()
	now = now.Add(time.Second)
	reporter.Complete(25)
	require.Equal(t, int64(0), reporter.totalBytes)
	require.Equal(t, int64(25), reporter.completedBytes)
	require.True(t, reporter.finished)

	defaultReporter := NewReporter(nil, nil, false, StageDownload, "default", 0)
	defaultReporter.Start()
	defaultReporter.Complete(0)
}

func TestReporterKeepsTerminalBarOffStructuredOutput(t *testing.T) {
	var structuredOutput bytes.Buffer
	var terminalOutput bytes.Buffer
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	reporter := newReporter(
		&structuredOutput,
		&terminalOutput,
		true,
		StageSourceRead,
		"artists",
		100,
		defaultReportInterval,
		func() time.Time { return now },
	)

	reporter.Start()
	now = now.Add(time.Second)
	reporter.Set(50)
	reporter.Fail(50)

	require.NotContains(t, structuredOutput.String(), "\x1b")
	require.NotContains(t, structuredOutput.String(), "[green]")
	require.Contains(t, terminalOutput.String(), "\x1b")
	require.NotContains(t, terminalOutput.String(), "[green]")
}

func TestReporterCompletesDownloadBar(t *testing.T) {
	var structuredOutput bytes.Buffer
	var terminalOutput bytes.Buffer
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	reporter := newReporter(
		&structuredOutput,
		&terminalOutput,
		true,
		StageDownload,
		"dump.xml.gz",
		100,
		defaultReportInterval,
		func() time.Time { return now },
	)

	reporter.Start()
	now = now.Add(time.Second)
	reporter.Complete(100)

	require.Contains(t, terminalOutput.String(), "writing dump.xml.gz...")
	require.Contains(t, terminalOutput.String(), "100%")
	require.NotContains(t, structuredOutput.String(), "\x1b")
}

func decodeRecords(t *testing.T, payload []byte) []Record {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(payload), []byte{'\n'})
	records := make([]Record, 0, len(lines))
	for _, line := range lines {
		var record Record
		require.NoError(t, json.Unmarshal(line, &record))
		records = append(records, record)
	}
	return records
}
