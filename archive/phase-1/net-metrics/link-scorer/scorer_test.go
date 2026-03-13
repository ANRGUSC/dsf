package main

import (
	"testing"
)

func TestComputePenalty(t *testing.T) {
	tests := []struct {
		name      string
		dropBps   float64
		dropRefBps float64
		want      float64
	}{
		{
			name:      "no drops",
			dropBps:   0,
			dropRefBps: 1e7,
			want:      1.0,
		},
		{
			name:      "half reference",
			dropBps:   5e6,
			dropRefBps: 1e7,
			want:      0.5,
		},
		{
			name:      "at reference",
			dropBps:   1e7,
			dropRefBps: 1e7,
			want:      0.1, // clamped to min
		},
		{
			name:      "above reference",
			dropBps:   2e7,
			dropRefBps: 1e7,
			want:      0.1, // clamped to min
		},
		{
			name:      "negative (should clamp)",
			dropBps:   -1e6,
			dropRefBps: 1e7,
			want:      1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computePenalty(tt.dropBps, tt.dropRefBps)
			if got != tt.want {
				t.Errorf("computePenalty(%v, %v) = %v, want %v", tt.dropBps, tt.dropRefBps, got, tt.want)
			}
		})
	}
}

func TestComputeScores(t *testing.T) {
	metrics := map[string]*NodeMetrics{
		"node1": {
			Node:            "node1",
			EgressBps:       2e8,  // 200 Mbps
			IngressBps:      1e8,  // 100 Mbps
			DropBps:         0,
			ResidualEgress:  8e8,  // 800 Mbps
			ResidualIngress: 9e8,  // 900 Mbps
			Penalty:         1.0,
		},
		"node2": {
			Node:            "node2",
			EgressBps:       3e8,  // 300 Mbps
			IngressBps:      2e8,  // 200 Mbps
			DropBps:         1e6,  // 1 Mbps
			ResidualEgress:  7e8,  // 700 Mbps
			ResidualIngress: 8e8,  // 800 Mbps
			Penalty:         computePenalty(1e6, 1e7), // ~0.9
		},
	}

	edges := computeScores(metrics)

	if len(edges) != 2 {
		t.Fatalf("Expected 2 edges, got %d", len(edges))
	}

	// node1 -> node2: min(800, 800) * 1.0 * 0.9 = 720
	// node2 -> node1: min(700, 900) * 0.9 * 1.0 = 630

	// Check sorting (highest first)
	if edges[0].Score < edges[1].Score {
		t.Errorf("Edges not sorted correctly")
	}

	// Verify score calculation
	for _, edge := range edges {
		src := metrics[edge.Src]
		dst := metrics[edge.Dst]
		expected := min(src.ResidualEgress, dst.ResidualIngress) * src.Penalty * dst.Penalty

		if edge.Score != expected {
			t.Errorf("Edge %s->%s: got %v, want %v", edge.Src, edge.Dst, edge.Score, expected)
		}
	}
}

func TestClamp(t *testing.T) {
	tests := []struct {
		name string
		val  float64
		min  float64
		max  float64
		want float64
	}{
		{"below min", 5, 10, 20, 10},
		{"above max", 25, 10, 20, 20},
		{"in range", 15, 10, 20, 15},
		{"at min", 10, 10, 20, 10},
		{"at max", 20, 10, 20, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clamp(tt.val, tt.min, tt.max)
			if got != tt.want {
				t.Errorf("clamp(%v, %v, %v) = %v, want %v", tt.val, tt.min, tt.max, got, tt.want)
			}
		})
	}
}

func TestMinMax(t *testing.T) {
	if min(5, 10) != 5 {
		t.Error("min(5, 10) should be 5")
	}
	if min(10, 5) != 5 {
		t.Error("min(10, 5) should be 5")
	}
	if max(5, 10) != 10 {
		t.Error("max(5, 10) should be 10")
	}
	if max(10, 5) != 10 {
		t.Error("max(10, 5) should be 10")
	}
}
