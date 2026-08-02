package bench

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	writeThroughputSVG    = "write-throughput.svg"
	pointReadLatencySVG   = "point-read-latency.svg"
	bloomFalsePositiveSVG = "bloom-false-positive.svg"
	amplificationSVG      = "amplification.svg"
	compactionPauseSVG    = "compaction-pause.svg"
	skiplistHeightSVG     = "skiplist-height.svg"

	svgWidth  = 960
	svgHeight = 540
)

type chartPoint struct {
	X int
	Y float64
}

type whisker struct {
	Min float64
	Max float64
}

type chartSeries struct {
	Name     string
	Color    string
	Points   []chartPoint
	Whiskers []whisker
}

// GenerateGraphs validates the complete raw result before atomically writing
// each of its deterministic SVG transforms.
func GenerateGraphs(result RawResult, outDir string) error {
	if outDir == "" {
		return fmt.Errorf("missing -out-dir")
	}
	if err := ValidateResult(result); err != nil {
		return err
	}
	charts := []struct {
		name string
		body string
	}{
		{writeThroughputSVG, renderWriteThroughput(result)},
		{pointReadLatencySVG, renderPointReadLatency(result)},
		{bloomFalsePositiveSVG, renderBloomFalsePositive(result)},
		{amplificationSVG, renderAmplification(result)},
		{compactionPauseSVG, renderCompactionPause(result)},
		{skiplistHeightSVG, renderSkiplistHeight(result)},
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, chart := range charts {
		if err := writeAtomicText(filepath.Join(outDir, chart.name), []byte(chart.body)); err != nil {
			return fmt.Errorf("write %s: %w", chart.name, err)
		}
	}
	return nil
}

// GenerateGraphsFromFile decodes, validates, and graphs one raw file.
func GenerateGraphsFromFile(path, outDir string) error {
	result, err := ReadRawResult(path)
	if err != nil {
		return err
	}
	return GenerateGraphs(result, outDir)
}

func renderWriteThroughput(result RawResult) string {
	byThreshold := make(map[int][]float64)
	for _, row := range result.WriteThroughput {
		byThreshold[row.FlushThresholdBytes] = append(byThreshold[row.FlushThresholdBytes], float64(row.Operations)*1e9/float64(row.ElapsedNS))
	}
	thresholds := sortedIntKeys(byThreshold)
	points := make([]chartPoint, 0, len(thresholds))
	whiskers := make([]whisker, 0, len(thresholds))
	labels := make([]string, 0, len(thresholds))
	for i, threshold := range thresholds {
		middle, low, high := aggregateMedianMinMax(byThreshold[threshold])
		points = append(points, chartPoint{X: i, Y: middle})
		whiskers = append(whiskers, whisker{Min: low, Max: high})
		labels = append(labels, strconv.Itoa(threshold))
	}
	return renderChart("Write throughput", labels, "Flush threshold (bytes)", "Operations per second", []chartSeries{{Name: "median ops/s", Color: "#1f77b4", Points: points, Whiskers: whiskers}})
}

func renderPointReadLatency(result RawResult) string {
	type cell struct {
		tables int
		bloom  bool
		kind   string
	}
	values := make(map[cell][]float64)
	for _, row := range result.PointReadLatency {
		key := cell{tables: row.SSTableCount, bloom: row.BloomEnabled, kind: row.LookupKind}
		values[key] = append(values[key], float64(row.P95NS))
	}
	tableCounts := sortedInts(result.Matrix.PointRead.SSTableCounts)
	labels := make([]string, len(tableCounts))
	for i, count := range tableCounts {
		labels[i] = strconv.Itoa(count)
	}
	combos := []struct {
		name  string
		color string
		bloom bool
		kind  string
	}{
		{"Bloom on hit", "#1f77b4", true, "hit"},
		{"Bloom off hit", "#2ca02c", false, "hit"},
		{"Bloom on miss", "#d62728", true, "miss"},
		{"Bloom off miss", "#9467bd", false, "miss"},
	}
	series := make([]chartSeries, 0, len(combos))
	for _, combo := range combos {
		points := make([]chartPoint, 0, len(tableCounts))
		whiskers := make([]whisker, 0, len(tableCounts))
		for x, count := range tableCounts {
			cellValues := sortedFloats(values[cell{tables: count, bloom: combo.bloom, kind: combo.kind}])
			points = append(points, chartPoint{X: x, Y: median(cellValues)})
			whiskers = append(whiskers, whisker{Min: cellValues[0], Max: cellValues[len(cellValues)-1]})
		}
		series = append(series, chartSeries{Name: combo.name, Color: combo.color, Points: points, Whiskers: whiskers})
	}
	return renderChart("Point-read latency", labels, "SSTable count", "P95 latency (ns)", series)
}

func renderBloomFalsePositive(result RawResult) string {
	measured := make(map[int][]float64)
	theory := make(map[int][]float64)
	for _, row := range result.BloomFalsePositive {
		measured[row.InsertedKeys] = append(measured[row.InsertedKeys], float64(row.FalsePositives)/float64(row.ProbeKeys))
		theory[row.InsertedKeys] = append(theory[row.InsertedKeys], bloomFalsePositiveTheory(row.InsertedKeys, row.MBits, float64(row.K)))
	}
	insertedKeys := sortedInts(result.Matrix.BloomFP.InsertedKeyCounts)
	labels := make([]string, len(insertedKeys))
	measuredPoints := make([]chartPoint, 0, len(insertedKeys))
	theoryPoints := make([]chartPoint, 0, len(insertedKeys))
	measuredWhiskers := make([]whisker, 0, len(insertedKeys))
	theoryWhiskers := make([]whisker, 0, len(insertedKeys))
	for i, inserted := range insertedKeys {
		labels[i] = strconv.Itoa(inserted)
		measuredValues := sortedFloats(measured[inserted])
		theoryValues := sortedFloats(theory[inserted])
		measuredPoints = append(measuredPoints, chartPoint{X: i, Y: median(measuredValues)})
		theoryPoints = append(theoryPoints, chartPoint{X: i, Y: median(theoryValues)})
		measuredWhiskers = append(measuredWhiskers, whisker{Min: measuredValues[0], Max: measuredValues[len(measuredValues)-1]})
		theoryWhiskers = append(theoryWhiskers, whisker{Min: theoryValues[0], Max: theoryValues[len(theoryValues)-1]})
	}
	return renderChart("Bloom false-positive rate", labels, "Inserted keys", "False-positive rate", []chartSeries{
		{Name: "measured", Color: "#1f77b4", Points: measuredPoints, Whiskers: measuredWhiskers},
		{Name: "theory from m, n, k", Color: "#ff7f0e", Points: theoryPoints, Whiskers: theoryWhiskers},
	})
}

func renderAmplification(result RawResult) string {
	panels := []struct {
		title string
		yAxis string
		value func(AmplificationRow) float64
	}{
		{"Write amplification", "(SSTable write bytes + MANIFEST write bytes) / logical user-write bytes", func(row AmplificationRow) float64 {
			return float64(row.SSTableWriteBytes+row.ManifestWriteBytes) / float64(row.LogicalUserWriteBytes)
		}},
		{"SSTable bytes per read", "SSTable read bytes / read operations", func(row AmplificationRow) float64 {
			return amplificationSSTableBytesPerRead(row)
		}},
		{"SSTable reads per read", "SSTable read calls / read operations", func(row AmplificationRow) float64 {
			return float64(row.SSTableReadCalls) / float64(row.ReadOperations)
		}},
		{"Space amplification", "Referenced SSTable bytes / logical live bytes", func(row AmplificationRow) float64 {
			return float64(row.ReferencedSSTableBytes) / float64(row.LogicalLiveBytes)
		}},
	}
	values := make([][]float64, len(panels))
	for _, row := range result.Amplification {
		for i, panel := range panels {
			values[i] = append(values[i], panel.value(row))
		}
	}
	var out bytes.Buffer
	out.WriteString(svgOpen("Amplification"))
	out.WriteString(renderText(480, 22, "Amplification", "#111827", 16, "middle"))
	for i, panel := range panels {
		left := 22.0 + float64(i%2)*468.0
		top := 42.0 + float64(i/2)*246.0
		out.WriteString(renderAggregatePanel(left, top, 448, 222, panel.title, panel.yAxis, sortedFloats(values[i])))
	}
	out.WriteString(`</svg>`)
	return out.String()
}

func amplificationSSTableBytesPerRead(row AmplificationRow) float64 {
	return float64(row.SSTableReadBytes) / float64(row.ReadOperations)
}

func aggregateMedianMinMax(values []float64) (medianValue, minValue, maxValue float64) {
	sorted := sortedFloats(values)
	return median(sorted), sorted[0], sorted[len(sorted)-1]
}

func renderCompactionPause(result RawResult) string {
	compacting := make([]float64, 0, len(result.CompactionPause))
	ordinary := make([]float64, 0, len(result.CompactionPause))
	for _, row := range result.CompactionPause {
		compacting = append(compacting, float64(row.CompactionP99NS))
		ordinary = append(ordinary, float64(row.OrdinaryP99NS))
	}
	compacting = sortedFloats(compacting)
	ordinary = sortedFloats(ordinary)
	return renderChart("Compaction pause", []string{"compaction-causing", "ordinary"}, "Operation class", "P99 latency (ns)", []chartSeries{
		{Name: "compaction-causing", Color: "#1f77b4", Points: []chartPoint{{X: 0, Y: median(compacting)}}, Whiskers: []whisker{{Min: compacting[0], Max: compacting[len(compacting)-1]}}},
		{Name: "ordinary", Color: "#2ca02c", Points: []chartPoint{{X: 1, Y: median(ordinary)}}, Whiskers: []whisker{{Min: ordinary[0], Max: ordinary[len(ordinary)-1]}}},
	})
}

func renderSkiplistHeight(result RawResult) string {
	measured := make([][]float64, 16)
	for _, row := range result.SkiplistHeight {
		for height, count := range row.HeightCounts {
			measured[height] = append(measured[height], float64(count)/float64(row.Samples))
		}
	}
	labels := make([]string, 16)
	measuredPoints := make([]chartPoint, 0, 16)
	theoryPoints := make([]chartPoint, 0, 16)
	measuredWhiskers := make([]whisker, 0, 16)
	for height := 0; height < 16; height++ {
		values := sortedFloats(measured[height])
		labels[height] = strconv.Itoa(height + 1)
		measuredPoints = append(measuredPoints, chartPoint{X: height, Y: median(values)})
		measuredWhiskers = append(measuredWhiskers, whisker{Min: values[0], Max: values[len(values)-1]})
		theory := 0.75 * math.Pow(0.25, float64(height))
		if height == 15 {
			theory = math.Pow(0.25, 15)
		}
		theoryPoints = append(theoryPoints, chartPoint{X: height, Y: theory})
	}
	return renderChart("Skip-list exact-height distribution", labels, "Exact height", "P(exact height)", []chartSeries{
		{Name: "measured exact-height probability", Color: "#1f77b4", Points: measuredPoints, Whiskers: measuredWhiskers},
		{Name: "theoretical exact-height probability", Color: "#ff7f0e", Points: theoryPoints},
	})
}

func renderAggregatePanel(left, top, width, height float64, title, yAxis string, values []float64) string {
	medianValue := median(values)
	minValue := values[0]
	maxValue := values[len(values)-1]
	maxY := maxValue
	if maxY <= 0 {
		maxY = 1
	}
	maxY *= 1.1
	plotLeft := left + 48
	plotTop := top + 44
	plotWidth := width - 62
	plotHeight := height - 78
	toY := func(v float64) float64 { return plotTop + plotHeight - v/maxY*plotHeight }
	center := plotLeft + plotWidth/2
	var out bytes.Buffer
	fmt.Fprintf(&out, `<g><rect x="%.3f" y="%.3f" width="%.3f" height="%.3f" fill="#ffffff" stroke="#d1d5db"/>`, left, top, width, height)
	out.WriteString(renderText(left+width/2, top+17, title, "#111827", 12, "middle"))
	out.WriteString(renderText(left+8, top+34, yAxis, "#374151", 8, "start"))
	for i := 0; i <= 2; i++ {
		y := plotTop + plotHeight*float64(i)/2
		out.WriteString(renderLine(plotLeft, y, plotLeft+plotWidth, y, "#e5e7eb", 1))
		out.WriteString(renderText(plotLeft-4, y+3, formatFloat(maxY*(1-float64(i)/2)), "#6b7280", 8, "end"))
	}
	out.WriteString(renderLine(plotLeft, plotTop, plotLeft, plotTop+plotHeight, "#9ca3af", 1))
	out.WriteString(renderLine(plotLeft, plotTop+plotHeight, plotLeft+plotWidth, plotTop+plotHeight, "#9ca3af", 1))
	fmt.Fprintf(&out, `<rect x="%.3f" y="%.3f" width="36.000" height="%.3f" fill="#1f77b4"/>`, center-18, toY(medianValue), plotTop+plotHeight-toY(medianValue))
	minY := toY(minValue)
	maxYCoord := toY(maxValue)
	out.WriteString(renderLine(center, minY, center, maxYCoord, "#111827", 1))
	out.WriteString(renderLine(center-5, minY, center+5, minY, "#111827", 1))
	out.WriteString(renderLine(center-5, maxYCoord, center+5, maxYCoord, "#111827", 1))
	out.WriteString(renderText(center, plotTop+plotHeight+15, "median across trials", "#6b7280", 9, "middle"))
	out.WriteString(`</g>`)
	return out.String()
}

func renderChart(title string, xLabels []string, xAxis, yAxis string, series []chartSeries) string {
	values := make([]float64, 0)
	for _, s := range series {
		for i, point := range s.Points {
			values = append(values, point.Y)
			if i < len(s.Whiskers) {
				values = append(values, s.Whiskers[i].Min, s.Whiskers[i].Max)
			}
		}
	}
	minY, maxY := chartBounds(values)
	plotLeft, plotTop := 88.0, 56.0
	plotWidth, plotHeight := 850.0, 426.0
	toX := func(index int) float64 {
		if len(xLabels) <= 1 {
			return plotLeft + plotWidth/2
		}
		return plotLeft + float64(index)*plotWidth/float64(len(xLabels)-1)
	}
	toY := func(value float64) float64 { return plotTop + plotHeight - (value-minY)/(maxY-minY)*plotHeight }
	var out bytes.Buffer
	out.WriteString(svgOpen(title))
	out.WriteString(renderText(480, 22, title, "#111827", 16, "middle"))
	out.WriteString(renderText(plotLeft, 39, yAxis, "#111827", 12, "start"))
	out.WriteString(renderText(940, 526, xAxis, "#111827", 12, "end"))
	for i := 0; i <= 5; i++ {
		y := plotTop + float64(i)*plotHeight/5
		value := maxY - float64(i)*(maxY-minY)/5
		out.WriteString(renderLine(plotLeft, y, plotLeft+plotWidth, y, "#e5e7eb", 1))
		out.WriteString(renderText(plotLeft-7, y+4, formatFloat(value), "#6b7280", 10, "end"))
	}
	for i, label := range xLabels {
		x := toX(i)
		out.WriteString(renderLine(x, plotTop+plotHeight, x, plotTop+plotHeight+4, "#9ca3af", 1))
		out.WriteString(renderText(x, plotTop+plotHeight+18, label, "#6b7280", 10, "middle"))
	}
	out.WriteString(renderLine(plotLeft, plotTop, plotLeft, plotTop+plotHeight, "#9ca3af", 1))
	out.WriteString(renderLine(plotLeft, plotTop+plotHeight, plotLeft+plotWidth, plotTop+plotHeight, "#9ca3af", 1))
	for _, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		coordinates := make([]string, 0, len(s.Points))
		for i, point := range s.Points {
			x, y := toX(point.X), toY(point.Y)
			coordinates = append(coordinates, fmt.Sprintf("%.3f %.3f", x, y))
			if i < len(s.Whiskers) {
				w := s.Whiskers[i]
				minW, maxW := toY(w.Min), toY(w.Max)
				out.WriteString(renderLine(x, minW, x, maxW, "#111827", 1))
				out.WriteString(renderLine(x-4, minW, x+4, minW, "#111827", 1))
				out.WriteString(renderLine(x-4, maxW, x+4, maxW, "#111827", 1))
			}
			out.WriteString(renderCircle(x, y, 3, s.Color))
		}
		if len(coordinates) > 1 {
			fmt.Fprintf(&out, `<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>`, s.Color, strings.Join(coordinates, " "))
		}
	}
	legendX, legendY := 600.0, 18.0
	for _, s := range series {
		out.WriteString(renderLine(legendX, legendY, legendX+16, legendY, s.Color, 2))
		out.WriteString(renderText(legendX+20, legendY+4, s.Name, "#111827", 10, "start"))
		legendY += 14
	}
	out.WriteString(`</svg>`)
	return out.String()
}

func chartBounds(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 1
	}
	minY, maxY := values[0], values[0]
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, 1
		}
		if value < minY {
			minY = value
		}
		if value > maxY {
			maxY = value
		}
	}
	if minY > 0 {
		minY = 0
	}
	if maxY <= minY {
		maxY = minY + 1
	}
	return minY, maxY
}

func svgOpen(label string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img" aria-label="%s"><rect x="0" y="0" width="%d" height="%d" fill="#ffffff"/>`, svgWidth, svgHeight, svgWidth, svgHeight, escapeText(label), svgWidth, svgHeight)
}

func sortedIntKeys(values map[int][]float64) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func sortedInts(values []int) []int {
	out := append([]int(nil), values...)
	sort.Ints(out)
	return out
}

func sortedFloats(values []float64) []float64 {
	out := append([]float64(nil), values...)
	sort.Float64s(out)
	return out
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func formatFloat(value float64) string { return strconv.FormatFloat(value, 'f', 6, 64) }

func renderText(x, y float64, label, fill string, size int, anchor string) string {
	return fmt.Sprintf(`<text x="%.3f" y="%.3f" text-anchor="%s" fill="%s" font-size="%d" font-family="Arial, sans-serif">%s</text>`, x, y, anchor, fill, size, escapeText(label))
}

func renderLine(x1, y1, x2, y2 float64, stroke string, width int) string {
	return fmt.Sprintf(`<line x1="%.3f" y1="%.3f" x2="%.3f" y2="%.3f" stroke="%s" stroke-width="%d" stroke-linecap="round"/>`, x1, y1, x2, y2, stroke, width)
}

func renderCircle(x, y, r float64, color string) string {
	return fmt.Sprintf(`<circle cx="%.3f" cy="%.3f" r="%.3f" fill="%s"/>`, x, y, r, color)
}

func bloomFalsePositiveTheory(inserted int, mBits uint64, k float64) float64 {
	if inserted <= 0 || mBits == 0 || k <= 0 {
		return 0
	}
	return math.Pow(1-math.Exp(-(k*float64(inserted))/float64(mBits)), k)
}

func escapeText(value string) string {
	return strings.NewReplacer("&", "&amp;", "\"", "&quot;", "'", "&apos;", "<", "&lt;", ">", "&gt;").Replace(value)
}
