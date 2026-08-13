package progress

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

const (
	EventByteProgress = "byte_progress"
	StageDownload     = "download"
	StageSourceRead   = "source_read"

	stateStarted   = "started"
	stateRunning   = "running"
	stateCompleted = "completed"
	stateFailed    = "failed"

	defaultReportInterval = 5 * time.Second
	metricPrecisionScale  = 100.0
)

type Record struct {
	Event          string  `json:"event"`
	State          string  `json:"state"`
	Stage          string  `json:"stage"`
	Resource       string  `json:"resource"`
	CompletedBytes int64   `json:"completed_bytes"`
	TotalBytes     int64   `json:"total_bytes"`
	Percent        float64 `json:"percent"`
	BytesPerSecond float64 `json:"bytes_per_second"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
}

type Reporter struct {
	mu             sync.Mutex
	output         io.Writer
	terminalOutput io.Writer
	bar            *progressbar.ProgressBar
	stage          string
	resource       string
	totalBytes     int64
	completedBytes int64
	startedAt      time.Time
	lastReportedAt time.Time
	interval       time.Duration
	now            func() time.Time
	finished       bool
}

func NewReporter(
	output io.Writer,
	terminalOutput io.Writer,
	terminal bool,
	stage string,
	resource string,
	totalBytes int64,
) *Reporter {
	return newReporter(
		output,
		terminalOutput,
		terminal,
		stage,
		resource,
		totalBytes,
		defaultReportInterval,
		time.Now,
	)
}

func newReporter(
	output io.Writer,
	terminalOutput io.Writer,
	terminal bool,
	stage string,
	resource string,
	totalBytes int64,
	interval time.Duration,
	now func() time.Time,
) *Reporter {
	if output == nil {
		output = io.Discard
	}
	if terminalOutput == nil {
		terminalOutput = io.Discard
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	reporter := &Reporter{
		output:         output,
		terminalOutput: terminalOutput,
		stage:          stage,
		resource:       resource,
		totalBytes:     totalBytes,
		interval:       interval,
		now:            now,
	}
	if terminal && totalBytes > 0 {
		reporter.bar = newBar(terminalOutput, stage, resource, totalBytes)
	}
	return reporter
}

func (r *Reporter) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.finished || !r.startedAt.IsZero() {
		return
	}
	now := r.now()
	r.startedAt = now
	r.lastReportedAt = now
	r.write(now, stateStarted)
	if r.bar != nil {
		_ = r.bar.RenderBlank()
	}
}

func (r *Reporter) Add(completedBytes int64) {
	if completedBytes <= 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.set(r.completedBytes+completedBytes, false)
}

func (r *Reporter) Set(completedBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.set(completedBytes, false)
}

func (r *Reporter) Complete(completedBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.set(completedBytes, true)
}

func (r *Reporter) Fail(completedBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	r.ensureStarted()
	r.completedBytes = r.normalizedCompletedBytes(completedBytes)
	r.updateBar()
	now := r.now()
	r.write(now, stateFailed)
	r.finishBar(true)
	r.finished = true
}

func (r *Reporter) set(completedBytes int64, forceComplete bool) {
	if r.finished {
		return
	}
	r.ensureStarted()
	r.completedBytes = r.normalizedCompletedBytes(completedBytes)
	r.updateBar()
	now := r.now()
	if forceComplete || (r.totalBytes > 0 && r.completedBytes == r.totalBytes) {
		r.write(now, stateCompleted)
		r.finishBar(false)
		r.finished = true
		return
	}
	if now.Sub(r.lastReportedAt) < r.interval {
		return
	}
	r.write(now, stateRunning)
	r.lastReportedAt = now
}

func (r *Reporter) updateBar() {
	if r.bar != nil {
		_ = r.bar.Set64(r.completedBytes)
	}
}

func (r *Reporter) finishBar(failed bool) {
	if r.bar == nil {
		return
	}
	if failed {
		_ = r.bar.Clear()
	} else {
		_ = r.bar.Finish()
	}
	_, _ = fmt.Fprintln(r.terminalOutput)
}

func (r *Reporter) ensureStarted() {
	if !r.startedAt.IsZero() {
		return
	}
	now := r.now()
	r.startedAt = now
	r.lastReportedAt = now
	r.write(now, stateStarted)
}

func (r *Reporter) normalizedCompletedBytes(completedBytes int64) int64 {
	if completedBytes < r.completedBytes {
		return r.completedBytes
	}
	if r.totalBytes > 0 && completedBytes > r.totalBytes {
		return r.totalBytes
	}
	return completedBytes
}

func (r *Reporter) write(now time.Time, state string) {
	elapsed := now.Sub(r.startedAt).Seconds()
	percent := 0.0
	if r.totalBytes > 0 {
		percent = float64(r.completedBytes) * 100 / float64(r.totalBytes)
	}
	bytesPerSecond := 0.0
	if elapsed > 0 {
		bytesPerSecond = float64(r.completedBytes) / elapsed
	}
	record := Record{
		Event:          EventByteProgress,
		State:          state,
		Stage:          r.stage,
		Resource:       r.resource,
		CompletedBytes: r.completedBytes,
		TotalBytes:     r.totalBytes,
		Percent:        RoundMetric(percent),
		BytesPerSecond: RoundMetric(bytesPerSecond),
		ElapsedSeconds: RoundMetric(elapsed),
	}
	payload, _ := json.Marshal(record)
	payload = append(payload, '\n')
	_, _ = r.output.Write(payload)
}

// RoundMetric bounds progress values without changing their numeric JSON type.
func RoundMetric(value float64) float64 {
	return math.Round(value*metricPrecisionScale) / metricPrecisionScale
}

func newBar(output io.Writer, stage, resource string, totalBytes int64) *progressbar.ProgressBar {
	description := resource
	if stage == StageDownload {
		description = fmt.Sprintf("writing %s...", resource)
	}
	return progressbar.NewOptions64(
		totalBytes,
		progressbar.OptionSetWriter(output),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionUseANSICodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionThrottle(250*time.Millisecond),
		progressbar.OptionSetWidth(15),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}
