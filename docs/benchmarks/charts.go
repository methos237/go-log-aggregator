//go:build ignore

// charts renders the SVG figures in docs/benchmarks/charts/ from the run.json
// files under runs/. Run it from the repository root after adding a run:
//
//	go run docs/benchmarks/charts.go
//
// Plain SVG, no dependencies, so the figures are reproducible from the committed
// numbers by anyone with a Go toolchain and render on GitHub without scripts. The
// tables in README.md are the accessible view of the same data.
//
// Style follows the dataviz skill: one hue for one series, thin bars with rounded
// data ends anchored on the baseline, recessive grid, text in text colors rather
// than the series color. Palette is the skill's validated default.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	surface   = "#fcfcfb"
	textMain  = "#0b0b0b"
	textMuted = "#52514e"
	grid      = "#e6e5e1"
	series1   = "#2a78d6"
)

type run struct {
	Loadgen struct {
		RecordsPerSec float64 `json:"records_per_sec"`
		AckP99Ms      float64 `json:"ack_p99_ms"`
	} `json:"loadgen"`
	EndToEnd struct {
		Sustained float64 `json:"sustained_records_per_sec"`
		DrainSec  float64 `json:"drain_sec"`
	} `json:"end_to_end"`
	Writer struct {
		MeanWriteMs   float64 `json:"mean_write_ms"`
		MeanBatchSize float64 `json:"mean_batch_size"`
	} `json:"writer"`
}

type bar struct {
	label string
	run   string // directory under runs/
}

type chart struct {
	file   string
	title  string
	unit   string
	value  func(run) float64
	format func(float64) string
	bars   []bar
}

func sustained(r run) float64 { return r.EndToEnd.Sustained }
func writePerThousand(r run) float64 {
	return r.Writer.MeanWriteMs / (r.Writer.MeanBatchSize / 1000)
}

func thousands(v float64) string {
	s := fmt.Sprintf("%.0f", v)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + " " + s[i:]
	}
	return s
}
func ms(v float64) string { return fmt.Sprintf("%.0f ms", v) }

var charts = []chart{
	{
		file:  "batch-size.svg",
		title: "Sustained end-to-end throughput by writer batch size",
		unit:  "records/s (baseline load shape)",
		value: sustained, format: thousands,
		bars: []bar{
			{"2 000", "wal4g-batch-2000"}, {"5 000 (default)", "wal-4g"},
			{"10 000", "wal4g-batch-10000"}, {"20 000", "wal4g-batch-20000"},
		},
	},
	{
		file:  "batch-size-write.svg",
		title: "Mean write time per 1 000 rows by writer batch size",
		unit:  "milliseconds, staging path, before the work_mem fix",
		value: writePerThousand, format: ms,
		bars: []bar{
			{"2 000", "wal4g-batch-2000"}, {"5 000 (default)", "wal-4g"},
			{"10 000", "wal4g-batch-10000"}, {"20 000", "wal4g-batch-20000"},
		},
	},
	{
		file:  "optimization-log.svg",
		title: "Sustained end-to-end throughput after each optimization",
		unit:  "records/s (baseline load shape), in the order the work happened",
		value: sustained, format: thousands,
		bars: []bar{
			{"baseline", "baseline"},
			{"WAL sizing", "wal-4g"},
			{"+ fewer allocs", "alloc"},
			{"+ work_mem", "staging-workmem"},
			{"+ direct COPY", "direct"},
			{"+ drop time idx", "direct-noidx"},
		},
	},
}

func main() {
	root := "docs/benchmarks"
	outDir := filepath.Join(root, "charts")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	for _, c := range charts {
		svg, err := render(root, c)
		if err != nil {
			log.Fatalf("%s: %v", c.file, err)
		}
		if err := os.WriteFile(filepath.Join(outDir, c.file), []byte(svg), 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Println("wrote", filepath.Join(outDir, c.file))
	}
}

func load(root, name string) (run, error) {
	var r run
	b, err := os.ReadFile(filepath.Join(root, "runs", name, "run.json"))
	if err != nil {
		return r, err
	}
	return r, json.Unmarshal(b, &r)
}

// render draws a single-series column chart. Bars that have no run yet are
// skipped rather than drawn at zero, so the figure can be regenerated as runs land.
func render(root string, c chart) (string, error) {
	type point struct {
		label string
		value float64
	}
	var pts []point
	for _, b := range c.bars {
		r, err := load(root, b.run)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		v := c.value(r)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			// A run whose counters did not add up (collector restarted mid-run,
			// missing metric family) has nothing to chart; skipping it beats a
			// NaN label or an axis loop that never ends.
			continue
		}
		pts = append(pts, point{b.label, v})
	}
	if len(pts) == 0 {
		return "", fmt.Errorf("no runs found")
	}

	const (
		width, height = 720, 360
		left, right   = 72, 24
		top, bottom   = 64, 56
	)
	plotW := float64(width - left - right)
	plotH := float64(height - top - bottom)
	maxV := 0.0
	for _, p := range pts {
		maxV = max(maxV, p.value)
	}
	step := niceStep(maxV / 4)
	axisMax := step * float64(int(maxV/step)+1)

	var sb strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&sb, format+"\n", a...) }
	w(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img" aria-label="%s">`,
		width, height, width, height, esc(c.title))
	w(`<style>text{font-family:-apple-system,"Segoe UI",Helvetica,Arial,sans-serif}</style>`)
	w(`<rect width="100%%" height="100%%" fill="%s"/>`, surface)
	w(`<text x="%d" y="28" font-size="15" font-weight="600" fill="%s">%s</text>`, left, textMain, esc(c.title))
	w(`<text x="%d" y="46" font-size="12" fill="%s">%s</text>`, left, textMuted, esc(c.unit))

	// Grid and y labels, recessive.
	for v := 0.0; v <= axisMax+1e-9; v += step {
		y := float64(top) + plotH - v/axisMax*plotH
		w(`<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>`, left, width-right, y, y, grid)
		w(`<text x="%d" y="%.1f" font-size="11" fill="%s" text-anchor="end" dominant-baseline="middle">%s</text>`,
			left-8, y, textMuted, thousands(v))
	}

	// Bars: thin, rounded top, 2px gap from neighbours, anchored on the baseline.
	slot := plotW / float64(len(pts))
	barW := min(slot*0.55, 72)
	base := float64(top) + plotH
	for i, p := range pts {
		x := float64(left) + slot*float64(i) + (slot-barW)/2
		h := p.value / axisMax * plotH
		if h < 4 {
			h = 4
		}
		y := base - h
		// Rounded data end only: a path with a 4px radius at the top corners.
		w(`<path d="M%.1f %.1f v%.1f a4 4 0 0 1 4 -4 h%.1f a4 4 0 0 1 4 4 v%.1f z" fill="%s"/>`,
			x, base, -(h - 4), barW-8, h-4, series1)
		w(`<text x="%.1f" y="%.1f" font-size="12" fill="%s" text-anchor="middle">%s</text>`,
			x+barW/2, y-6, textMain, esc(c.format(p.value)))
		w(`<text x="%.1f" y="%d" font-size="12" fill="%s" text-anchor="middle">%s</text>`,
			x+barW/2, height-bottom+20, textMuted, esc(p.label))
	}
	w(`<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>`, left, width-right, base, base, textMuted)
	w(`</svg>`)
	return sb.String(), nil
}

// niceStep rounds a raw step up to 1, 2 or 5 times a power of ten.
func niceStep(raw float64) float64 {
	if raw <= 0 {
		return 1
	}
	mag := 1.0
	for mag*10 <= raw {
		mag *= 10
	}
	for _, m := range []float64{1, 2, 5, 10} {
		if raw <= m*mag {
			return m * mag
		}
	}
	return 10 * mag
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;").Replace(s)
}
