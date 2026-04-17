package app

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type progressTracker struct {
	total        int
	current      int
	label        string
	done         chan struct{}
	mu           sync.Mutex
	active       bool
	mode         progressMode
	countCurrent int
	countTotal   int
}

type progressMode int

const (
	progressModeSteps progressMode = iota
	progressModeCount
)

func newProgressTracker(total int) *progressTracker {
	if !isInteractiveTerminal() {
		return &progressTracker{total: total}
	}
	return &progressTracker{total: total, active: true, mode: progressModeSteps}
}

func (p *progressTracker) Start(stepLabel string) {
	if !p.active {
		return
	}
	p.mu.Lock()
	p.current++
	p.label = stepLabel
	p.mode = progressModeSteps
	done := make(chan struct{})
	p.done = done
	p.mu.Unlock()

	go func() {
		frames := []string{"-", "\\", "|", "/"}
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-done:
				p.render("done")
				fmt.Fprintln(os.Stdout)
				return
			case <-ticker.C:
				p.render(frames[i % len(frames)])
				i++
			}
		}
	}()
}

func (p *progressTracker) StartCount(stepLabel string) {
	if !p.active {
		return
	}
	p.mu.Lock()
	p.label = stepLabel
	p.mode = progressModeCount
	p.countCurrent = 0
	p.countTotal = 0
	done := make(chan struct{})
	p.done = done
	p.mu.Unlock()

	go func() {
		frames := []string{"-", "\\", "|", "/"}
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-done:
				p.render("done")
				fmt.Fprintln(os.Stdout)
				return
			case <-ticker.C:
				p.render(frames[i%len(frames)])
				i++
			}
		}
	}()
}

func (p *progressTracker) UpdateCount(current, total int) {
	if !p.active {
		return
	}
	p.mu.Lock()
	p.countCurrent = current
	p.countTotal = total
	p.mu.Unlock()
}

func (p *progressTracker) End() {
	if !p.active {
		return
	}
	p.mu.Lock()
	done := p.done
	p.done = nil
	p.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func (p *progressTracker) render(spinner string) {
	p.mu.Lock()
	mode := p.mode
	step := p.current
	total := p.total
	label := p.label
	countCurrent := p.countCurrent
	countTotal := p.countTotal
	p.mu.Unlock()

	width := 20
	filled := 0
	switch mode {
	case progressModeCount:
		if countTotal > 0 {
			filled = int(float64(countCurrent) / float64(countTotal) * float64(width))
		}
		if filled > width {
			filled = width
		}
		bar := strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
		if countTotal > 0 {
			percent := int(float64(countCurrent) / float64(countTotal) * 100)
			if percent > 100 {
				percent = 100
			}
			fmt.Fprintf(os.Stdout, "\r[%s] %d/%d %3d%% %s %s", bar, countCurrent, countTotal, percent, spinner, label)
			return
		}
		fmt.Fprintf(os.Stdout, "\r[%s] %d issues %s %s", strings.Repeat("-", width), countCurrent, spinner, label)
	default:
		if total > 0 {
			filled = int(float64(step) / float64(total) * float64(width))
		}
		if filled > width {
			filled = width
		}
		bar := strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
		fmt.Fprintf(os.Stdout, "\r[%s] %d/%d %s %s", bar, step, total, spinner, label)
	}
}

func (p *progressTracker) Finish() {
	p.End()
}
