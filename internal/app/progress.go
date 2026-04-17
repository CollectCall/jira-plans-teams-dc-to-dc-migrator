package app

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type progressTracker struct {
	total          int
	active         bool
	mu             sync.Mutex
	tasks          map[string]*progressTaskState
	order          []string
	doneCount      int
	nextTaskID     int
	renderDone     chan struct{}
	renderFinished chan struct{}
	renderLines    int
	legacyTaskID   string
}

type progressTaskState struct {
	id      string
	label   string
	current int
	total   int
	done    bool
	err     error
}

type progressTask struct {
	tracker *progressTracker
	id      string
}

func newProgressTracker(total int) *progressTracker {
	p := &progressTracker{
		total:  total,
		active: isInteractiveTerminal(),
		tasks:  map[string]*progressTaskState{},
	}
	if p.active {
		p.startRenderer()
	}
	return p
}

func (p *progressTracker) BeginTask(label string) *progressTask {
	if !p.active {
		return &progressTask{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.nextTaskID++
	id := fmt.Sprintf("task-%d", p.nextTaskID)
	p.tasks[id] = &progressTaskState{id: id, label: label}
	p.order = append(p.order, id)
	return &progressTask{tracker: p, id: id}
}

func (p *progressTracker) Start(stepLabel string) {
	task := p.BeginTask(stepLabel)
	p.legacyTaskID = task.id
}

func (p *progressTracker) StartCount(stepLabel string) {
	task := p.BeginTask(stepLabel)
	p.legacyTaskID = task.id
}

func (p *progressTracker) UpdateCount(current, total int) {
	if p.legacyTaskID == "" {
		return
	}
	p.updateTask(p.legacyTaskID, current, total)
}

func (p *progressTracker) End() {
	if p.legacyTaskID == "" {
		return
	}
	p.finishTask(p.legacyTaskID, nil)
	p.legacyTaskID = ""
}

func (p *progressTracker) Finish() {
	if !p.active {
		return
	}

	p.mu.Lock()
	for _, id := range p.order {
		task := p.tasks[id]
		if task != nil && !task.done {
			task.done = true
			p.doneCount++
		}
	}
	renderDone := p.renderDone
	renderFinished := p.renderFinished
	p.mu.Unlock()

	if renderDone != nil {
		close(renderDone)
	}
	if renderFinished != nil {
		<-renderFinished
	}
}

func (p *progressTracker) startRenderer() {
	p.renderDone = make(chan struct{})
	p.renderFinished = make(chan struct{})

	go func() {
		defer close(p.renderFinished)
		frames := []string{"-", "\\", "|", "/"}
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()

		i := 0
		for {
			select {
			case <-p.renderDone:
				p.render(frames[i%len(frames)])
				p.clearRender()
				return
			case <-ticker.C:
				p.render(frames[i%len(frames)])
				i++
			}
		}
	}()
}

func (p *progressTracker) render(spinner string) {
	overall, taskLines := p.snapshotLines(spinner)
	lines := append([]string{overall}, taskLines...)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.renderLines > 0 {
		for i := 0; i < p.renderLines; i++ {
			fmt.Fprint(os.Stdout, "\r\x1b[2K")
			if i < p.renderLines-1 {
				fmt.Fprint(os.Stdout, "\x1b[1A")
			}
		}
		fmt.Fprint(os.Stdout, "\r")
	}

	for i, line := range lines {
		if i > 0 {
			fmt.Fprint(os.Stdout, "\n")
		}
		fmt.Fprint(os.Stdout, line)
	}
	p.renderLines = len(lines)
}

func (p *progressTracker) clearRender() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.renderLines == 0 {
		return
	}
	for i := 0; i < p.renderLines; i++ {
		fmt.Fprint(os.Stdout, "\r\x1b[2K")
		if i < p.renderLines-1 {
			fmt.Fprint(os.Stdout, "\x1b[1A")
		}
	}
	fmt.Fprint(os.Stdout, "\r")
	p.renderLines = 0
}

func (p *progressTracker) snapshotLines(spinner string) (string, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := p.total
	if total <= 0 {
		total = maxInt(1, len(p.order))
	}
	done := p.doneCount
	if done > total {
		done = total
	}

	width := 20
	filled := int(float64(done) / float64(total) * float64(width))
	if filled > width {
		filled = width
	}
	bar := strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
	overall := fmt.Sprintf("[%s] %d/%d done %s", bar, done, total, spinner)

	active := make([]string, 0, 4)
	for _, id := range p.order {
		task := p.tasks[id]
		if task == nil || task.done {
			continue
		}
		active = append(active, formatTaskLine(task, spinner))
		if len(active) == 4 {
			break
		}
	}

	if remaining := activeTaskCountLocked(p.tasks) - len(active); remaining > 0 {
		active = append(active, fmt.Sprintf("  ... %d more active task(s)", remaining))
	}

	return overall, active
}

func activeTaskCountLocked(tasks map[string]*progressTaskState) int {
	count := 0
	for _, task := range tasks {
		if task != nil && !task.done {
			count++
		}
	}
	return count
}

func formatTaskLine(task *progressTaskState, spinner string) string {
	width := 16
	if task.total > 0 {
		filled := int(float64(task.current) / float64(task.total) * float64(width))
		if filled > width {
			filled = width
		}
		bar := strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
		percent := int(float64(task.current) / float64(task.total) * 100)
		if percent > 100 {
			percent = 100
		}
		return fmt.Sprintf("  [%s] %d/%d %3d%% %s", bar, task.current, task.total, percent, task.label)
	}
	if task.current > 0 {
		return fmt.Sprintf("  [%s] %d items %s", strings.Repeat("-", width), task.current, task.label)
	}
	return fmt.Sprintf("  [%s] %s %s", strings.Repeat("-", width), spinner, task.label)
}

func (p *progressTracker) updateTask(id string, current, total int) {
	if !p.active {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	task := p.tasks[id]
	if task == nil || task.done {
		return
	}
	task.current = current
	task.total = total
}

func (p *progressTracker) finishTask(id string, err error) {
	if !p.active {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	task := p.tasks[id]
	if task == nil || task.done {
		return
	}
	task.done = true
	task.err = err
	p.doneCount++
}

func (t *progressTask) Update(current, total int) {
	if t == nil || t.tracker == nil {
		return
	}
	t.tracker.updateTask(t.id, current, total)
}

func (t *progressTask) Done() {
	if t == nil || t.tracker == nil {
		return
	}
	t.tracker.finishTask(t.id, nil)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
