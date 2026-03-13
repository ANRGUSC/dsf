package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var (
	dataDir = "/data/dag-outputs"
	port    = 8080
)

func main() {
	flag.StringVar(&dataDir, "data-dir", dataDir, "Directory to serve files from")
	flag.IntVar(&port, "port", port, "Port to listen on")
	flag.Parse()

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory %s: %v", dataDir, err)
	}

	// Get node name from environment
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}

	// Create file server
	fs := http.FileServer(http.Dir(dataDir))

	// Wrap with logging
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[%s] %s %s", nodeName, r.Method, r.URL.Path)

		// Check if file exists
		fullPath := filepath.Join(dataDir, r.URL.Path)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}

		fs.ServeHTTP(w, r)
	})

	// Health check endpoint
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok")
	})

	// List all available data endpoint
	http.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		var files []string
		filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				relPath, _ := filepath.Rel(dataDir, path)
				files = append(files, relPath)
			}
			return nil
		})

		w.Header().Set("Content-Type", "text/plain")
		for _, f := range files {
			fmt.Fprintf(w, "%s\n", f)
		}
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Data Agent starting on %s, serving %s (node: %s)", addr, dataDir, nodeName)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
