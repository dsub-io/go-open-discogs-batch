package batch

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/progress"
	"io"
	"os"
	"sync"
	"time"
)

const (
	importProgressEvent         = "import_progress"
	progressStateStarted        = "started"
	progressStateRunning        = "running"
	progressStateCompleted      = "completed"
	progressStateFailed         = "failed"
	progressStateObservationErr = "observation_error"
	defaultProgressInterval     = 5 * time.Second
)

type committedProgress struct {
	ProcessedItems int64
	TotalItems     sql.NullInt64
	LastProgressAt sql.NullTime
}

type importProgressRecord struct {
	Event                 string     `json:"event"`
	State                 string     `json:"state"`
	Entity                string     `json:"entity"`
	CommittedItems        int64      `json:"committed_items"`
	CommittedPercent      *float64   `json:"committed_percent,omitempty"`
	RowsPerSecond         float64    `json:"rows_per_second"`
	ElapsedSeconds        float64    `json:"elapsed_seconds"`
	Resumed               bool       `json:"resumed"`
	InitialCommittedItems int64      `json:"initial_committed_items"`
	LastCommittedProgress *time.Time `json:"last_committed_progress_at,omitempty"`
	ObservationError      string     `json:"observation_error,omitempty"`
}

type progressSummaryReader func() (committedProgress, error)

type entityProgressReporter interface {
	Start()
	Observe()
	Finish(bool)
}

type noopProgressReporter struct{}

func (noopProgressReporter) Start()      {}
func (noopProgressReporter) Observe()    {}
func (noopProgressReporter) Finish(bool) {}

type jsonProgressReporter struct {
	mu             sync.Mutex
	entity         string
	resumed        bool
	interval       time.Duration
	now            func() time.Time
	readSummary    progressSummaryReader
	output         io.Writer
	startedAt      time.Time
	lastReportedAt time.Time
	initialItems   int64
	baselineSet    bool
}

func newEntityProgressReporter(order Order) entityProgressReporter {
	if order.getRunID() == 0 {
		return noopProgressReporter{}
	}
	return &jsonProgressReporter{
		entity:   order.getEntityType(),
		resumed:  order.shouldResumeProgress(),
		interval: defaultProgressInterval,
		now:      time.Now,
		readSummary: func() (committedProgress, error) {
			return readCommittedProgress(order)
		},
		output: progress.StructuredOutput(os.Stdout),
	}
}

func (r *jsonProgressReporter) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.startedAt = now
	r.lastReportedAt = now
	summary, err := r.readSummary()
	if err != nil {
		r.writeObservationError(now, err)
		return
	}
	r.initialItems = summary.ProcessedItems
	r.baselineSet = true
	r.writeRecord(now, progressStateStarted, summary)
}

func (r *jsonProgressReporter) Observe() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.lastReportedAt) < r.interval {
		return
	}
	r.lastReportedAt = now
	summary, err := r.readSummary()
	if err != nil {
		r.writeObservationError(now, err)
		return
	}
	r.setBaselineIfNeeded(summary)
	r.writeRecord(now, progressStateRunning, summary)
}

func (r *jsonProgressReporter) Finish(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	summary, err := r.readSummary()
	if err != nil {
		r.writeObservationError(now, err)
		return
	}
	r.setBaselineIfNeeded(summary)
	state := progressStateFailed
	if success {
		state = progressStateCompleted
	}
	r.writeRecord(now, state, summary)
}

func (r *jsonProgressReporter) setBaselineIfNeeded(summary committedProgress) {
	if r.baselineSet {
		return
	}
	r.initialItems = summary.ProcessedItems
	r.baselineSet = true
}

func (r *jsonProgressReporter) writeRecord(
	now time.Time,
	state string,
	summary committedProgress,
) {
	elapsed := now.Sub(r.startedAt).Seconds()
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(summary.ProcessedItems-r.initialItems) / elapsed
	}
	var committedPercent *float64
	if summary.TotalItems.Valid && summary.TotalItems.Int64 > 0 {
		percent := float64(summary.ProcessedItems) * 100 / float64(summary.TotalItems.Int64)
		committedPercent = &percent
	} else if summary.TotalItems.Valid && summary.TotalItems.Int64 == 0 {
		percent := float64(100)
		committedPercent = &percent
	}
	var lastProgress *time.Time
	if summary.LastProgressAt.Valid {
		lastProgress = &summary.LastProgressAt.Time
	}
	r.write(importProgressRecord{
		Event:                 importProgressEvent,
		State:                 state,
		Entity:                r.entity,
		CommittedItems:        summary.ProcessedItems,
		CommittedPercent:      committedPercent,
		RowsPerSecond:         rate,
		ElapsedSeconds:        elapsed,
		Resumed:               r.resumed,
		InitialCommittedItems: r.initialItems,
		LastCommittedProgress: lastProgress,
	})
}

func (r *jsonProgressReporter) writeObservationError(now time.Time, err error) {
	r.write(importProgressRecord{
		Event:            importProgressEvent,
		State:            progressStateObservationErr,
		Entity:           r.entity,
		ElapsedSeconds:   now.Sub(r.startedAt).Seconds(),
		Resumed:          r.resumed,
		ObservationError: err.Error(),
	})
}

func (r *jsonProgressReporter) write(record importProgressRecord) {
	_ = json.NewEncoder(r.output).Encode(record)
}

func readCommittedProgress(order Order) (committedProgress, error) {
	var summary committedProgress
	query := order.getDB().WithContext(order.getContext()).Raw(
		`select processed_items, total_items, last_progress_at
		   from discogs_import_run_dump
		  where import_run_id = ?
		    and entity_type = ?`,
		order.getRunID(),
		order.getEntityType(),
	).Scan(&summary)
	if query.Error != nil {
		return committedProgress{}, query.Error
	}
	if query.RowsAffected != 1 {
		return committedProgress{}, errors.New("import progress summary is unavailable")
	}
	return summary, nil
}
