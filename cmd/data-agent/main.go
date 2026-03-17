// data-agent: per-node HTTP agent for p2p file transport and task state
// tracking in ODAGs.
//
// Data endpoints (body = raw bytes):
//   PUT /<odag>/<task>/output  — upstream task pushes output to this node
//   GET /<odag>/<task>/output  — read output (debug / fallback)
//
// State endpoints (body = plain text state name):
//   PUT /state/<odag>/<task>   — task SDK reports its current state
//   GET /state/<odag>/<task>   — controller queries task state
//
// States: Executing | Sending | DataReady | Done | Failed
//
// GET /healthz  — liveness probe
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		dataDir = "/data/dsf-outputs"
		port    = 8081
	)
	flag.StringVar(&dataDir, "data-dir", dataDir, "directory to serve")
	flag.IntVar(&port, "port", port, "listen port")
	flag.Parse()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("[data-agent] failed to create data dir %s: %v", dataDir, err)
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}

	fs := http.FileServer(http.Dir(dataDir))

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	// Sending endpoints: /sending/<odag>/<task>
	// Sending files are stored alongside data: <dataDir>/<odag>/<task>/.dsf-sending
	http.HandleFunc("/sending/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/sending/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		sendingFile := filepath.Join(dataDir, filepath.Clean(rel), ".dsf-sending")

		switch r.Method {
		case http.MethodPut:
			if err := os.MkdirAll(filepath.Dir(sendingFile), 0755); err != nil {
				http.Error(w, "failed to create directory", http.StatusInternalServerError)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			if err := os.WriteFile(sendingFile, body, 0644); err != nil {
				http.Error(w, "failed to write sending", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] SENDING %s = %s", nodeName, rel, strings.TrimSpace(string(body)))
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			data, err := os.ReadFile(sendingFile)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read sending", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// State endpoints: /state/<odag>/<task>
	// State files are stored alongside data: <dataDir>/<odag>/<task>/.dsf-state
	http.HandleFunc("/state/", func(w http.ResponseWriter, r *http.Request) {
		// Strip "/state/" prefix to get "<odag>/<task>"
		rel := strings.TrimPrefix(r.URL.Path, "/state/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		stateFile := filepath.Join(dataDir, filepath.Clean(rel), ".dsf-state")

		switch r.Method {
		case http.MethodPut:
			if err := os.MkdirAll(filepath.Dir(stateFile), 0755); err != nil {
				http.Error(w, "failed to create directory", http.StatusInternalServerError)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			if err := os.WriteFile(stateFile, body, 0644); err != nil {
				http.Error(w, "failed to write state", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] STATE %s = %s", nodeName, rel, strings.TrimSpace(string(body)))
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			data, err := os.ReadFile(stateFile)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read state", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Data endpoints: /<odag>/<task>/output
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cleanPath := filepath.Clean(r.URL.Path)
		fullPath := filepath.Join(dataDir, cleanPath)

		switch r.Method {
		case http.MethodPut:
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				http.Error(w, "failed to create directory", http.StatusInternalServerError)
				return
			}
			f, err := os.Create(fullPath)
			if err != nil {
				http.Error(w, "failed to create file", http.StatusInternalServerError)
				return
			}
			n, err := io.Copy(f, r.Body)
			f.Close()
			if err != nil {
				http.Error(w, "failed to write file", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] PUT %s (%d bytes)", nodeName, r.URL.Path, n)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			if _, err := os.Stat(fullPath); os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			log.Printf("[data-agent/%s] GET %s", nodeName, r.URL.Path)
			fs.ServeHTTP(w, r)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("[data-agent] node=%s serving %s on %s", nodeName, dataDir, addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("[data-agent] %v", err)
	}
}
