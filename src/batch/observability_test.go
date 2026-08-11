package batch

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestNoopProgressReporter(t *testing.T) {
	noop := noopProgressReporter{}
	noop.Start()
	noop.Observe()
	noop.Finish(true)

	reporter := newEntityProgressReporter(NewOrder(
		context.Background(),
		1,
		1,
		"unused",
		nil,
	))

	reporter.Start()
	reporter.Observe()
	reporter.Finish(true)
	reporter.Finish(false)
}

func TestJSONProgressReporterLifecycle(t *testing.T) {
	start := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	now := start
	lastProgress := start.Add(-time.Minute)
	summaries := []committedProgress{
		{
			ProcessedItems: 40,
			LastProgressAt: sql.NullTime{Time: lastProgress, Valid: true},
		},
		{ProcessedItems: 60},
		{
			ProcessedItems: 100,
			TotalItems:     sql.NullInt64{Int64: 100, Valid: true},
		},
	}
	readIndex := 0
	var output bytes.Buffer
	reporter := &jsonProgressReporter{
		entity:   "release",
		resumed:  true,
		interval: defaultProgressInterval,
		now:      func() time.Time { return now },
		readSummary: func() (committedProgress, error) {
			summary := summaries[readIndex]
			readIndex++
			return summary, nil
		},
		output: &output,
	}

	reporter.Start()
	reporter.Observe()
	now = now.Add(defaultProgressInterval)
	reporter.Observe()
	now = now.Add(defaultProgressInterval)
	reporter.Finish(true)

	records := decodeProgressRecords(t, output.Bytes())
	require.Len(t, records, 3)
	require.Equal(t, progressStateStarted, records[0].State)
	require.Equal(t, int64(40), records[0].CommittedItems)
	require.Equal(t, int64(40), records[0].InitialCommittedItems)
	require.Equal(t, lastProgress, *records[0].LastCommittedProgress)
	require.Nil(t, records[0].CommittedPercent)
	require.Equal(t, progressStateRunning, records[1].State)
	require.InDelta(t, 4, records[1].RowsPerSecond, 0.001)
	require.Equal(t, progressStateCompleted, records[2].State)
	require.InDelta(t, 100, *records[2].CommittedPercent, 0.001)
	require.InDelta(t, 6, records[2].RowsPerSecond, 0.001)
}

func TestJSONProgressReporterFailureAndObservationErrors(t *testing.T) {
	start := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	now := start
	expected := errors.New("fixture")
	responses := []struct {
		summary committedProgress
		err     error
	}{
		{err: expected},
		{err: expected},
		{err: expected},
		{
			summary: committedProgress{
				TotalItems: sql.NullInt64{Valid: true},
			},
		},
	}
	responseIndex := 0
	var output bytes.Buffer
	reporter := &jsonProgressReporter{
		entity:   "artist",
		interval: defaultProgressInterval,
		now:      func() time.Time { return now },
		readSummary: func() (committedProgress, error) {
			response := responses[responseIndex]
			responseIndex++
			return response.summary, response.err
		},
		output: &output,
	}

	reporter.Start()
	now = now.Add(defaultProgressInterval)
	reporter.Observe()
	reporter.Finish(true)
	reporter.Finish(false)

	records := decodeProgressRecords(t, output.Bytes())
	require.Len(t, records, 4)
	for index := 0; index < 3; index++ {
		require.Equal(t, progressStateObservationErr, records[index].State)
		require.Equal(t, expected.Error(), records[index].ObservationError)
	}
	require.Equal(t, progressStateFailed, records[3].State)
	require.InDelta(t, 100, *records[3].CommittedPercent, 0.001)
	require.Zero(t, records[3].InitialCommittedItems)
	require.Zero(t, records[3].RowsPerSecond)
}

func TestJSONProgressReporterRecoversBaselineAfterStartObservationError(t *testing.T) {
	start := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	now := start
	readCount := 0
	var output bytes.Buffer
	reporter := &jsonProgressReporter{
		entity:   "master",
		interval: defaultProgressInterval,
		now:      func() time.Time { return now },
		readSummary: func() (committedProgress, error) {
			readCount++
			if readCount == 1 {
				return committedProgress{}, errors.New("fixture")
			}
			return committedProgress{
				ProcessedItems: 25,
				TotalItems:     sql.NullInt64{Int64: 25, Valid: true},
			}, nil
		},
		output: &output,
	}

	reporter.Start()
	now = now.Add(defaultProgressInterval)
	reporter.Observe()

	records := decodeProgressRecords(t, output.Bytes())
	require.Len(t, records, 2)
	require.Equal(t, progressStateRunning, records[1].State)
	require.Equal(t, int64(25), records[1].InitialCommittedItems)
	require.Zero(t, records[1].RowsPerSecond)
}

func TestReadCommittedProgress(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		now := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
		mock.ExpectQuery("select processed_items, total_items, last_progress_at").
			WithArgs(int64(7), "master").
			WillReturnRows(sqlmock.NewRows(
				[]string{"processed_items", "total_items", "last_progress_at"},
			).AddRow(25, nil, now))
		order := NewTrackedOrder(
			context.Background(), 5, 1, "unused", db, 7, "master", true,
		)

		summary, err := readCommittedProgress(order)

		require.NoError(t, err)
		require.Equal(t, int64(25), summary.ProcessedItems)
		require.False(t, summary.TotalItems.Valid)
		require.Equal(t, now, summary.LastProgressAt.Time)
	})

	t.Run("query failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expected := errors.New("fixture")
		mock.ExpectQuery("select processed_items, total_items, last_progress_at").
			WithArgs(int64(8), "label").
			WillReturnError(expected)
		order := NewTrackedOrder(
			context.Background(), 5, 1, "unused", db, 8, "label", false,
		)

		_, err := readCommittedProgress(order)

		require.ErrorIs(t, err, expected)
	})

	t.Run("missing summary", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select processed_items, total_items, last_progress_at").
			WithArgs(int64(9), "artist").
			WillReturnRows(sqlmock.NewRows(
				[]string{"processed_items", "total_items", "last_progress_at"},
			))
		order := NewTrackedOrder(
			context.Background(), 5, 1, "unused", db, 9, "artist", false,
		)

		_, err := readCommittedProgress(order)

		require.ErrorContains(t, err, "summary is unavailable")
	})
}

func TestTrackedOrderProgressReporterUsesDurableSummary(t *testing.T) {
	db, mock, _ := newMockGorm(t)
	mock.ExpectQuery("select processed_items, total_items, last_progress_at").
		WithArgs(int64(10), "release").
		WillReturnRows(sqlmock.NewRows(
			[]string{"processed_items", "total_items", "last_progress_at"},
		).AddRow(3, nil, nil))
	reporter := newEntityProgressReporter(NewTrackedOrder(
		context.Background(), 1, 1, "unused", db, 10, "release", true,
	))

	reporter.Start()
}

func decodeProgressRecords(t *testing.T, payload []byte) []importProgressRecord {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	records := make([]importProgressRecord, 0)
	for decoder.More() {
		var record importProgressRecord
		require.NoError(t, decoder.Decode(&record))
		records = append(records, record)
	}
	return records
}
