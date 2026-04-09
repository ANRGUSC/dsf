// data-agent: per-node HTTP agent for p2p file transport and task state
// tracking in ODAGs.
//
// Data endpoints (body = raw bytes):
//   PUT /<odag>/<task>/output        — upstream task pushes output to this node
//   GET /<odag>/<task>/output        — read output (debug / fallback)
//
// State endpoints (body = plain text state name):
//   PUT /state/<odag>/<task>         — task SDK reports its current state
//   GET /state/<odag>/<task>         — controller queries task state
//
// Sending endpoints:
//   PUT /sending/<odag>/<task>       — set sending flag (body: "true"/"false")
//   GET /sending/<odag>/<task>       — query sending flag
//
// Push endpoint (option-3 decoupled transfer):
//   POST /push/<odag>/<task>         — ask agent to push local output to remote
//                                      successor nodes; responds 200 immediately
//                                      and completes transfer in background.
//                                      Body: JSON {"successors":[{"name":"x","host":"1.2.3.4"},...]}
//
// States: Executing | DataReady | Done | Failed
//
// GET /healthz  — liveness probe
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	pushRetries    = 5
	pushRetryDelay = 500 * time.Millisecond
	pushTimeout    = 120 * time.Second
	dataAgentPort  = 8081
)

var dataDir string

func stateFile(rel string) string   { return filepath.Join(dataDir, filepath.Clean(rel), ".dsf-state") }
func sendingFile(rel string) string { return filepath.Join(dataDir, filepath.Clean(rel), ".dsf-sending") }
func bytesFile(rel string) string   { return filepath.Join(dataDir, filepath.Clean(rel), ".dsf-bytes") }

// dirSize walks a directory tree and returns total bytes.
func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// runInfo is returned by GET /runs — one entry per ODAG directory on this node.
type runInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"` // total bytes
}

func writeFile(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(value), 0644)
}

func setState(rel, state string) {
	if err := writeFile(stateFile(rel), state); err != nil {
		log.Printf("[data-agent] setState %s=%s: %v", rel, state, err)
	}
}

func setSending(rel string, sending bool) {
	val := "false"
	if sending {
		val = "true"
	}
	if err := writeFile(sendingFile(rel), val); err != nil {
		log.Printf("[data-agent] setSending %s=%s: %v", rel, val, err)
	}
}

func pushToNode(odag, task, host string, data []byte) error {
	url := fmt.Sprintf("http://%s:%d/%s/%s/output", host, dataAgentPort, odag, task)
	client := &http.Client{Timeout: pushTimeout}
	for attempt := 1; attempt <= pushRetries; attempt++ {
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if err != nil {
			log.Printf("[data-agent] push to %s attempt %d/%d: %v", host, attempt, pushRetries, err)
		} else {
			resp.Body.Close()
			log.Printf("[data-agent] push to %s attempt %d/%d: status %d", host, attempt, pushRetries, resp.StatusCode)
		}
		if attempt < pushRetries {
			time.Sleep(pushRetryDelay)
		}
	}
	return fmt.Errorf("push to %s failed after %d attempts", host, pushRetries)
}

func main() {
	port := dataAgentPort
	flag.StringVar(&dataDir, "data-dir", "/data/dsf-outputs", "directory to serve")
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

	// POST /push/<odag>/<task>
	// Reads local output file and pushes to each remote successor in a goroutine.
	// Responds 200 immediately; the pod can exit right away.
	http.HandleFunc("/push/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rel := strings.TrimPrefix(r.URL.Path, "/push/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) != 2 {
			http.Error(w, "expected odag/task path", http.StatusBadRequest)
			return
		}
		odag, task := parts[0], parts[1]

		var body struct {
			Successors []struct {
				Name string `json:"name"`
				Host string `json:"host"`
			} `json:"successors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)

		// If no remote successors, set DataReady immediately and done.
		if len(body.Successors) == 0 {
			setState(rel, "DataReady")
			log.Printf("[data-agent/%s] PUSH %s/%s: no remote successors, DataReady", nodeName, odag, task)
			return
		}

		// Otherwise push in background so the pod can exit immediately.
		go func() {
			localFile := filepath.Join(dataDir, odag, task, "output")
			data, err := os.ReadFile(localFile)
			if err != nil {
				log.Printf("[data-agent/%s] PUSH %s/%s: failed to read local file: %v", nodeName, odag, task, err)
				setState(rel, "Failed")
				return
			}

			// Record actual output size.
			_ = writeFile(bytesFile(rel), fmt.Sprintf("%d", len(data)))

			setSending(rel, true)
			log.Printf("[data-agent/%s] PUSH %s/%s: pushing %d bytes to %d successor(s)",
				nodeName, odag, task, len(data), len(body.Successors))

			allOK := true
			for _, succ := range body.Successors {
				log.Printf("[data-agent/%s] PUSH %s/%s -> %s (%s)", nodeName, odag, task, succ.Name, succ.Host)
				if err := pushToNode(odag, task, succ.Host, data); err != nil {
					log.Printf("[data-agent/%s] PUSH %s/%s -> %s FAILED: %v", nodeName, odag, task, succ.Name, err)
					allOK = false
				} else {
					log.Printf("[data-agent/%s] PUSH %s/%s -> %s OK", nodeName, odag, task, succ.Name)
				}
			}

			setSending(rel, false)
			if allOK {
				setState(rel, "DataReady")
				log.Printf("[data-agent/%s] PUSH %s/%s: DataReady", nodeName, odag, task)
			} else {
				setState(rel, "Failed")
				log.Printf("[data-agent/%s] PUSH %s/%s: Failed (one or more pushes failed)", nodeName, odag, task)
			}
		}()
	})

	// GET /bytes/<odag>/<task> — query actual output bytes for a task
	http.HandleFunc("/bytes/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/bytes/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data, err := os.ReadFile(bytesFile(rel))
		if os.IsNotExist(err) {
			fmt.Fprint(w, "0")
			return
		}
		if err != nil {
			http.Error(w, "failed to read", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(data)
	})

	// PUT/GET /sending/<odag>/<task>
	http.HandleFunc("/sending/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/sending/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		sf := sendingFile(rel)
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			if err := writeFile(sf, strings.TrimSpace(string(body))); err != nil {
				http.Error(w, "failed to write", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] SENDING %s = %s", nodeName, rel, strings.TrimSpace(string(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, err := os.ReadFile(sf)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// PUT/GET /state/<odag>/<task>
	http.HandleFunc("/state/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/state/")
		if rel == "" {
			http.Error(w, "missing odag/task path", http.StatusBadRequest)
			return
		}
		sf := stateFile(rel)
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusInternalServerError)
				return
			}
			if err := writeFile(sf, strings.TrimSpace(string(body))); err != nil {
				http.Error(w, "failed to write", http.StatusInternalServerError)
				return
			}
			log.Printf("[data-agent/%s] STATE %s = %s", nodeName, rel, strings.TrimSpace(string(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, err := os.ReadFile(sf)
			if os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "failed to read", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(data)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /runs — list all ODAG directories on this node with their sizes.
	// Optional query param ?prefix=<template> filters to runs matching that prefix.
	// Response: JSON array of {"name":"<odag>","size":<bytes>} sorted by name.
	http.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")

		entries, err := os.ReadDir(dataDir)
		if err != nil {
			http.Error(w, "failed to read data dir", http.StatusInternalServerError)
			return
		}

		var runs []runInfo
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if prefix != "" && !strings.HasPrefix(name, prefix) {
				continue
			}
			size := dirSize(filepath.Join(dataDir, name))
			runs = append(runs, runInfo{Name: name, Size: size})
		}
		if runs == nil {
			runs = []runInfo{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(runs)
	})

	// DELETE /data/<odag> — remove all data for a specific ODAG run on this node.
	http.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		odag := strings.TrimPrefix(r.URL.Path, "/data/")
		odag = strings.TrimSuffix(odag, "/")
		if odag == "" || strings.Contains(odag, "/") || strings.Contains(odag, "..") {
			http.Error(w, "invalid odag name", http.StatusBadRequest)
			return
		}
		target := filepath.Join(dataDir, odag)
		if _, err := os.Stat(target); os.IsNotExist(err) {
			w.WriteHeader(http.StatusOK) // idempotent
			log.Printf("[data-agent/%s] DELETE %s: not found (already clean)", nodeName, odag)
			return
		}
		if err := os.RemoveAll(target); err != nil {
			http.Error(w, "failed to remove", http.StatusInternalServerError)
			log.Printf("[data-agent/%s] DELETE %s: %v", nodeName, odag, err)
			return
		}
		log.Printf("[data-agent/%s] DELETE %s: removed", nodeName, odag)
		w.WriteHeader(http.StatusOK)
	})

	// PUT/GET /<odag>/<task>/output
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
			// If this is a pushed output file (<odag>/<task>/output), set DataReady
			// on this node so the controller knows the data has arrived here.
			if strings.HasSuffix(cleanPath, "/output") {
				rel := strings.TrimPrefix(strings.TrimSuffix(cleanPath, "/output"), "/")
				setState(rel, "DataReady")
				log.Printf("[data-agent/%s] PUT %s (%d bytes) → DataReady", nodeName, r.URL.Path, n)
			} else {
				log.Printf("[data-agent/%s] PUT %s (%d bytes)", nodeName, r.URL.Path, n)
			}
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
