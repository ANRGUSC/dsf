package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultWindow      = "2m"
	defaultCapacityBps = 1e9
	defaultDropRefBps  = 1e7
	defaultTopK        = 20
)

type Config struct {
	PromURL     string
	PromToken   string
	Window      string
	CapacityBps float64
	DropRefBps  float64
	Output      string
	TopK        int
	Nodes       []string
}

type NodeMetrics struct {
	Node          string
	EgressBps    float64
	IngressBps   float64
	DropBps      float64
	ResidualEgress  float64
	ResidualIngress float64
	Penalty      float64
}

type EdgeScore struct {
	Src   string
	Dst   string
	Score float64
}

type Output struct {
	Timestamp   string                  `json:"timestamp"`
	Window      string                  `json:"window"`
	Nodes       map[string]*NodeMetrics `json:"nodes"`
	Edges       []EdgeScore             `json:"edges"`
	TopK        int                     `json:"top_k,omitempty"`
}

type PrometheusClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewPrometheusClient(baseURL, token string) *PrometheusClient {
	return &PrometheusClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		token:   token,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (p *PrometheusClient) Query(query string) (map[string]float64, error) {
	u := fmt.Sprintf("%s/api/v1/query", p.baseURL)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("query", query)
	req.URL.RawQuery = q.Encode()

	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("prometheus returned %d: %s", resp.StatusCode, string(body))
	}

	var promResp PrometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&promResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus status: %s", promResp.Status)
	}

	result := make(map[string]float64)
	for _, r := range promResp.Data.Result {
		// Try 'node' label first (from Prometheus relabeling), fallback to 'instance'
		instance := r.Metric["node"]
		if instance == "" {
			instance = r.Metric["instance"]
		}
		if instance == "" {
			continue
		}

		if len(r.Value) < 2 {
			continue
		}

		valStr, ok := r.Value[1].(string)
		if !ok {
			continue
		}

		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Printf("WARN: failed to parse value for %s: %v", instance, err)
			continue
		}

		result[instance] = val
	}

	return result, nil
}

func clamp(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func computePenalty(dropBps, dropRefBps float64) float64 {
	return clamp(1.0-dropBps/dropRefBps, 0.1, 1.0)
}

func computeMetrics(client *PrometheusClient, config *Config) (map[string]*NodeMetrics, error) {
	window := config.Window
	if window == "" {
		window = defaultWindow
	}

	// Use 'instance' label (from PodMonitor relabeling)
	egressQuery := fmt.Sprintf(`sum by (instance) (8 * rate(networkobservability_forward_bytes{direction="egress"}[%s]))`, window)
	ingressQuery := fmt.Sprintf(`sum by (instance) (8 * rate(networkobservability_forward_bytes{direction="ingress"}[%s]))`, window)
	dropQuery := fmt.Sprintf(`sum by (instance) (8 * rate(networkobservability_drop_bytes[%s]))`, window)

	log.Printf("Querying egress metrics...")
	egressBps, err := client.Query(egressQuery)
	if err != nil {
		log.Printf("WARN: egress query failed: %v", err)
		egressBps = make(map[string]float64)
	}

	log.Printf("Querying ingress metrics...")
	ingressBps, err := client.Query(ingressQuery)
	if err != nil {
		log.Printf("WARN: ingress query failed: %v", err)
		ingressBps = make(map[string]float64)
	}

	log.Printf("Querying drop metrics...")
	dropBps, err := client.Query(dropQuery)
	if err != nil {
		log.Printf("WARN: drop query failed (may not exist): %v", err)
		dropBps = make(map[string]float64)
	}

	allNodes := make(map[string]bool)
	for node := range egressBps {
		allNodes[node] = true
	}
	for node := range ingressBps {
		allNodes[node] = true
	}
	for node := range dropBps {
		allNodes[node] = true
	}
	if len(config.Nodes) > 0 {
		allNodes = make(map[string]bool)
		for _, node := range config.Nodes {
			allNodes[node] = true
		}
	}

	metrics := make(map[string]*NodeMetrics)
	for node := range allNodes {
		egress := egressBps[node]
		ingress := ingressBps[node]
		drops := dropBps[node]

		residualEgress := max(0, config.CapacityBps-egress)
		residualIngress := max(0, config.CapacityBps-ingress)
		penalty := computePenalty(drops, config.DropRefBps)

		metrics[node] = &NodeMetrics{
			Node:            node,
			EgressBps:       egress,
			IngressBps:      ingress,
			DropBps:         drops,
			ResidualEgress:  residualEgress,
			ResidualIngress: residualIngress,
			Penalty:         penalty,
		}
	}

	return metrics, nil
}

func computeScores(metrics map[string]*NodeMetrics) []EdgeScore {
	var edges []EdgeScore

	nodes := make([]string, 0, len(metrics))
	for node := range metrics {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	for _, src := range nodes {
		srcMetrics := metrics[src]
		for _, dst := range nodes {
			if src == dst {
				continue
			}
			dstMetrics := metrics[dst]

			score := min(srcMetrics.ResidualEgress, dstMetrics.ResidualIngress) *
				srcMetrics.Penalty * dstMetrics.Penalty

			edges = append(edges, EdgeScore{
				Src:   src,
				Dst:   dst,
				Score: score,
			})
		}
	}

	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Score > edges[j].Score
	})

	return edges
}

func outputJSON(out *Output) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func outputCSV(edges []EdgeScore) error {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	if err := w.Write([]string{"src", "dst", "score"}); err != nil {
		return err
	}

	for _, edge := range edges {
		if err := w.Write([]string{
			edge.Src,
			edge.Dst,
			strconv.FormatFloat(edge.Score, 'f', 2, 64),
		}); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	config := &Config{}

	flag.StringVar(&config.PromURL, "prom-url", "", "Prometheus URL (or set PROM_URL)")
	flag.StringVar(&config.PromToken, "prom-token", "", "Prometheus bearer token (or set PROM_TOKEN)")
	flag.StringVar(&config.Window, "window", defaultWindow, "Time window for rate calculation (or set WINDOW)")
	flag.Float64Var(&config.CapacityBps, "capacity", defaultCapacityBps, "Link capacity in bps (or set CAPACITY_BPS)")
	flag.Float64Var(&config.DropRefBps, "drop-ref", defaultDropRefBps, "Reference drop rate for penalty (or set DROP_REF_BPS)")
	flag.StringVar(&config.Output, "output", "json", "Output format: json or csv (or set OUTPUT)")
	flag.IntVar(&config.TopK, "topk", defaultTopK, "Top K edges to output (0 = all) (or set TOPK)")
	flag.Func("nodes", "Comma-separated node list (or set NODES)", func(s string) error {
		config.Nodes = strings.Split(s, ",")
		for i := range config.Nodes {
			config.Nodes[i] = strings.TrimSpace(config.Nodes[i])
		}
		return nil
	})

	flag.Parse()

	if config.PromURL == "" {
		config.PromURL = os.Getenv("PROM_URL")
	}
	if config.PromToken == "" {
		config.PromToken = os.Getenv("PROM_TOKEN")
	}
	if config.Window == defaultWindow {
		if w := os.Getenv("WINDOW"); w != "" {
			config.Window = w
		}
	}
	if env := os.Getenv("CAPACITY_BPS"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			config.CapacityBps = val
		}
	}
	if env := os.Getenv("DROP_REF_BPS"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			config.DropRefBps = val
		}
	}
	if env := os.Getenv("OUTPUT"); env != "" {
		config.Output = env
	}
	if env := os.Getenv("TOPK"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			config.TopK = val
		}
	}
	if len(config.Nodes) == 0 {
		if nodes := os.Getenv("NODES"); nodes != "" {
			config.Nodes = strings.Split(nodes, ",")
			for i := range config.Nodes {
				config.Nodes[i] = strings.TrimSpace(config.Nodes[i])
			}
		}
	}

	if config.PromURL == "" {
		log.Fatal("PROM_URL is required (flag or env var)")
	}

	if _, err := url.Parse(config.PromURL); err != nil {
		log.Fatalf("Invalid PROM_URL: %v", err)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client := NewPrometheusClient(config.PromURL, config.PromToken)

	metrics, err := computeMetrics(client, config)
	if err != nil {
		log.Fatalf("Failed to compute metrics: %v", err)
	}

	if len(metrics) == 0 {
		log.Fatal("No node metrics found")
	}

	edges := computeScores(metrics)

	if config.TopK > 0 && len(edges) > config.TopK {
		edges = edges[:config.TopK]
	}

	if config.Output == "csv" {
		if err := outputCSV(edges); err != nil {
			log.Fatalf("Failed to output CSV: %v", err)
		}
	} else {
		out := &Output{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Window:    config.Window,
			Nodes:     metrics,
			Edges:     edges,
		}
		if config.TopK > 0 {
			out.TopK = config.TopK
		}
		if err := outputJSON(out); err != nil {
			log.Fatalf("Failed to output JSON: %v", err)
		}
	}
}
