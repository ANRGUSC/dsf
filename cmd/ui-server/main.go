package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	_ "modernc.org/sqlite"
)

// --------------------------------------------------------------------------
// GVRs
// --------------------------------------------------------------------------

var (
	odagGVR         = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "odags"}
	cdagGVR         = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "cdags"}
	odagTemplateGVR = schema.GroupVersionResource{Group: "dsf.io", Version: "v1", Resource: "odagtemplates"}
)

// --------------------------------------------------------------------------
// In-memory cache (updated by the K8s watch loop)
// --------------------------------------------------------------------------

type Server struct {
	dynClient dynamic.Interface
	db        *sql.DB
	mu        sync.RWMutex
	odags     map[string]*unstructured.Unstructured // "ns/name" -> obj
	cdags     map[string]*unstructured.Unstructured
	templates map[string]*unstructured.Unstructured

	// SSE clients: each client gets a channel of JSON event bytes.
	sseMu      sync.Mutex
	sseClients map[chan []byte]struct{}
}

func newServer(dynClient dynamic.Interface, db *sql.DB) *Server {
	return &Server{
		dynClient:  dynClient,
		db:         db,
		odags:      make(map[string]*unstructured.Unstructured),
		cdags:      make(map[string]*unstructured.Unstructured),
		templates:  make(map[string]*unstructured.Unstructured),
		sseClients: make(map[chan []byte]struct{}),
	}
}

// --------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------

func main() {
	var kubeconfig, addr, dbPath string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	flag.StringVar(&addr, "addr", envOrDefault("DSF_LISTEN_ADDR", ":8080"), "listen address")
	flag.StringVar(&dbPath, "db", envOrDefault("DSF_DB_PATH", "/data/dsf-history.db"), "SQLite database path")
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		log.Fatalf("[ui-server] failed to build config: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[ui-server] failed to create dynamic client: %v", err)
	}

	db, err := openDB(dbPath)
	if err != nil {
		log.Fatalf("[ui-server] failed to open database: %v", err)
	}
	defer db.Close()

	srv := newServer(dynClient, db)

	// Start K8s watch loops.
	go srv.watchResources(odagGVR, &srv.odags)
	go srv.watchResources(cdagGVR, &srv.cdags)
	go srv.watchResources(odagTemplateGVR, &srv.templates)

	// HTTP routes.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/odags", srv.handleListODAGs)
	mux.HandleFunc("GET /api/odags/{namespace}/{name}", srv.handleGetODAG)
	mux.HandleFunc("GET /api/odags/{namespace}/{name}/history", srv.handleGetODAGHistory)
	mux.HandleFunc("POST /api/odags/{namespace}/{name}/retry", srv.handleRetryODAG)
	mux.HandleFunc("GET /api/cdags", srv.handleListCDAGs)
	mux.HandleFunc("GET /api/cdags/{namespace}/{name}", srv.handleGetCDAG)
	mux.HandleFunc("POST /api/batch", srv.handleBatchSubmit)
	mux.HandleFunc("GET /api/templates", srv.handleListTemplates)
	mux.HandleFunc("GET /api/templates/{namespace}/{name}", srv.handleGetTemplate)
	mux.HandleFunc("GET /api/templates/{namespace}/{name}/runs", srv.handleGetTemplateRuns)
	mux.HandleFunc("POST /api/templates/{namespace}/{name}/run", srv.handleRunTemplate)
	mux.HandleFunc("DELETE /api/templates/{namespace}/{name}", srv.handleDeleteTemplate)
	mux.HandleFunc("GET /api/events", srv.handleSSE)

	// Serve compiled React frontend from ui/dist (embedded at build time).
	// During development, the Vite dev server proxies /api to this server.
	// SPA fallback: serve index.html for any path not matched by a file, so
	// that React Router handles client-side routes on hard refresh.
	uiDir := envOrDefault("DSF_UI_DIR", "./ui/dist")
	if _, err := os.Stat(uiDir); err == nil {
		fs := http.FileServer(http.Dir(uiDir))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// Never serve the SPA for /api/ paths — return 404 JSON so the
			// browser gets a clear error rather than HTML it can't parse.
			if strings.HasPrefix(r.URL.Path, "/api/") {
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			// Serve real static assets (JS, CSS, fonts) directly.
			// no-cache ensures the browser always revalidates so it picks
			// up new content-hashed bundles without a hard refresh.
			if r.URL.Path != "/" {
				if _, err := os.Stat(uiDir + r.URL.Path); err == nil {
					w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
					fs.ServeHTTP(w, r)
					return
				}
			}
			// SPA fallback: all other paths get index.html so React Router
			// can handle them. No-cache so the browser always picks up the
			// latest content-hashed bundle filename after deploys.
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			http.ServeFile(w, r, uiDir+"/index.html")
		})
		log.Printf("[ui-server] serving frontend from %s", uiDir)
	} else {
		log.Printf("[ui-server] no frontend found at %s; serving API only", uiDir)
	}

	log.Printf("[ui-server] listening on %s", addr)
	if err := http.ListenAndServe(addr, cors(mux)); err != nil {
		log.Fatalf("[ui-server] %v", err)
	}
}

// --------------------------------------------------------------------------
// K8s watch loop
// --------------------------------------------------------------------------

func (s *Server) watchResources(gvr schema.GroupVersionResource, cache *map[string]*unstructured.Unstructured) {
	for {
		watcher, err := s.dynClient.Resource(gvr).Namespace("").Watch(
			context.Background(), metav1.ListOptions{},
		)
		if err != nil {
			log.Printf("[ui-server] error watching %s: %v; retrying in 5s", gvr.Resource, err)
			time.Sleep(5 * time.Second)
			continue
		}
		for event := range watcher.ResultChan() {
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			key := obj.GetNamespace() + "/" + obj.GetName()

			s.mu.Lock()
			switch string(event.Type) {
			case "ADDED", "MODIFIED":
				(*cache)[key] = obj
				// Record completion in history when an ODAG reaches a terminal phase.
				if gvr == odagGVR {
					phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
					if phase == "Succeeded" || phase == "Failed" {
						go s.recordHistory(obj)
					}
				}
			case "DELETED":
				delete(*cache, key)
			}
			s.mu.Unlock()

			// Notify SSE clients.
			s.broadcast(gvr.Resource, string(event.Type), obj.GetName(), obj.GetNamespace())
		}
		time.Sleep(2 * time.Second)
	}
}

// --------------------------------------------------------------------------
// REST handlers
// --------------------------------------------------------------------------

func (s *Server) handleListODAGs(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(s.odags))
	for _, obj := range s.odags {
		result = append(result, odagSummary(obj))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["name"]) < fmt.Sprint(result[j]["name"])
	})
	writeJSON(w, result)
}

func (s *Server) handleGetODAG(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	s.mu.RLock()
	obj, ok := s.odags[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, odagDetail(obj))
}

func (s *Server) handleGetODAGHistory(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	history, err := s.queryHistory(ns, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, history)
}

func (s *Server) handleRetryODAG(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	ctx := r.Context()

	// Fetch current object to extract spec.
	existing, err := s.dynClient.Resource(odagGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	spec, _, _ := unstructured.NestedMap(existing.Object, "spec")

	// Delete the existing ODAG.
	if err := s.dynClient.Resource(odagGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond immediately — recreation happens in background so the browser
	// returns quickly and the SSE stream drives the live graph updates.
	writeJSON(w, map[string]string{"status": "ok"})

	go func() {
		bgCtx := context.Background()
		fresh := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "dsf.io/v1",
				"kind":       "ODAG",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": ns,
				},
				"spec": spec,
			},
		}
		// Wait up to 10s for the object to be gone, then recreate.
		for i := 0; i < 20; i++ {
			time.Sleep(500 * time.Millisecond)
			_, err := s.dynClient.Resource(odagGVR).Namespace(ns).Get(bgCtx, name, metav1.GetOptions{})
			if err != nil {
				break
			}
		}
		if _, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(bgCtx, fresh, metav1.CreateOptions{}); err != nil {
			log.Printf("[ui-server] retry create failed for %s/%s: %v", ns, name, err)
		}
	}()
}

func (s *Server) handleListCDAGs(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(s.cdags))
	for _, obj := range s.cdags {
		result = append(result, cdagSummary(obj))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["name"]) < fmt.Sprint(result[j]["name"])
	})
	writeJSON(w, result)
}

func (s *Server) handleGetCDAG(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	s.mu.RLock()
	obj, ok := s.cdags[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, cdagDetail(obj))
}

// --------------------------------------------------------------------------
// Template handlers
// --------------------------------------------------------------------------

func (s *Server) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(s.templates))
	for _, obj := range s.templates {
		result = append(result, templateSummary(obj))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["name"]) < fmt.Sprint(result[j]["name"])
	})
	writeJSON(w, result)
}

func (s *Server) handleGetTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	s.mu.RLock()
	obj, ok := s.templates[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, templateDetail(obj))
}

func (s *Server) handleGetTemplateRuns(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")

	s.mu.RLock()
	var runs []map[string]interface{}
	for _, obj := range s.odags {
		labels := obj.GetLabels()
		if labels["dsf.io/template"] == name && obj.GetNamespace() == ns {
			phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
			makespan := nestedFloat(obj.Object, "status", "makespan")
			startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
			completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
			runs = append(runs, map[string]interface{}{
				"name":           obj.GetName(),
				"namespace":      ns,
				"run":            labels["dsf.io/run"],
				"phase":          defaultStr(phase, "Pending"),
				"makespan":       makespan,
				"startTime":      startTime,
				"completionTime": completionTime,
				"createdAt":      obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
			})
		}
	}
	s.mu.RUnlock()

	sort.Slice(runs, func(i, j int) bool {
		return fmt.Sprint(runs[i]["createdAt"]) < fmt.Sprint(runs[j]["createdAt"])
	})
	if runs == nil {
		runs = []map[string]interface{}{}
	}
	writeJSON(w, runs)
}

func (s *Server) handleRunTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")

	s.mu.RLock()
	tmplObj, ok := s.templates[ns+"/"+name]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "template not found", http.StatusNotFound)
		return
	}

	// Find the highest existing run number to determine the next one.
	s.mu.RLock()
	maxRun := 0
	for _, obj := range s.odags {
		labels := obj.GetLabels()
		if labels["dsf.io/template"] == name && obj.GetNamespace() == ns {
			if n, err := strconv.Atoi(labels["dsf.io/run"]); err == nil && n > maxRun {
				maxRun = n
			}
		}
	}
	s.mu.RUnlock()
	runNum := maxRun + 1
	odagName := fmt.Sprintf("%s-run-%03d", name, runNum)

	// Extract spec from template, stripping template-only fields.
	spec, _, _ := unstructured.NestedMap(tmplObj.Object, "spec")
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	odag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dsf.io/v1",
			"kind":       "ODAG",
			"metadata": map[string]interface{}{
				"name":      odagName,
				"namespace": ns,
				"labels": map[string]interface{}{
					"dsf.io/template": name,
					"dsf.io/run":      fmt.Sprintf("%d", runNum),
				},
			},
			"spec": spec,
		},
	}

	if _, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(
		context.Background(), odag, metav1.CreateOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"name":    odagName,
		"run":     runNum,
		"message": fmt.Sprintf("Created run %s from template %s", odagName, name),
	})
}

func (s *Server) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	if err := s.dynClient.Resource(odagTemplateGVR).Namespace(ns).Delete(
		context.Background(), name, metav1.DeleteOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted", "name": name})
}

// --------------------------------------------------------------------------
// Template response builders
// --------------------------------------------------------------------------

func templateSummary(obj *unstructured.Unstructured) map[string]interface{} {
	tasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	sched, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	desc, _, _ := unstructured.NestedString(obj.Object, "spec", "description")
	runCount := nestedFloat(obj.Object, "status", "runCount")
	lastMakespan := nestedFloat(obj.Object, "status", "lastRunMakespan")
	lastRunName, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunName")
	lastRunPhase, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunPhase")

	profilingEnabled := true
	if v, ok, _ := unstructured.NestedBool(obj.Object, "spec", "profiling", "enabled"); ok {
		profilingEnabled = v
	}

	return map[string]interface{}{
		"name":             obj.GetName(),
		"namespace":        obj.GetNamespace(),
		"description":      desc,
		"scheduler":        defaultStr(sched, "random"),
		"taskCount":        len(tasks),
		"runCount":         int(runCount),
		"lastRunMakespan":  lastMakespan,
		"lastRunName":      lastRunName,
		"lastRunPhase":     lastRunPhase,
		"profilingEnabled": profilingEnabled,
		"createdAt":        obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
	}
}

func templateDetail(obj *unstructured.Unstructured) map[string]interface{} {
	summary := templateSummary(obj)
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	profiling, _, _ := unstructured.NestedMap(obj.Object, "spec", "profiling")
	defaults, _, _ := unstructured.NestedMap(obj.Object, "spec", "defaults")
	retention, _, _ := unstructured.NestedMap(obj.Object, "spec", "retention")
	profileSummary, _, _ := unstructured.NestedMap(obj.Object, "status", "profileSummary")

	summary["spec"] = map[string]interface{}{
		"tasks":     specTasks,
		"profiling": profiling,
		"defaults":  defaults,
		"retention": retention,
	}
	if profileSummary != nil {
		summary["profileSummary"] = profileSummary
	}
	return summary
}

// handleBatchSubmit creates multiple ODAGs with staggered delays.
func (s *Server) handleBatchSubmit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
		ODAGs     []struct {
			Name  string                 `json:"name"`
			Delay int                    `json:"delay"`
			Spec  map[string]interface{} `json:"spec"`
		} `json:"odags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ns := req.Namespace
	if ns == "" {
		ns = "dsf-system"
	}

	bgCtx := context.Background()

	// Delete existing ODAGs with these names.
	for _, o := range req.ODAGs {
		_ = s.dynClient.Resource(odagGVR).Namespace(ns).Delete(bgCtx, o.Name, metav1.DeleteOptions{})
	}

	// Respond immediately.
	writeJSON(w, map[string]interface{}{"status": "started", "count": len(req.ODAGs)})

	// Submit with staggered delays in background.
	go func() {
		// Wait for deletions to propagate.
		time.Sleep(3 * time.Second)

		for _, o := range req.ODAGs {
			if o.Delay > 0 {
				time.Sleep(time.Duration(o.Delay) * time.Second)
			}
			cr := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dsf.io/v1",
					"kind":       "ODAG",
					"metadata": map[string]interface{}{
						"name":      o.Name,
						"namespace": ns,
					},
					"spec": o.Spec,
				},
			}
			if _, err := s.dynClient.Resource(odagGVR).Namespace(ns).Create(bgCtx, cr, metav1.CreateOptions{}); err != nil {
				log.Printf("[ui-server] batch create failed for %s: %v", o.Name, err)
			} else {
				log.Printf("[ui-server] batch submitted %s (delay=%ds)", o.Name, o.Delay)
			}
		}
		log.Printf("[ui-server] batch submission complete (%d ODAGs)", len(req.ODAGs))
	}()
}

// handleSSE streams live resource change events to the frontend.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	s.sseMu.Lock()
	s.sseClients[ch] = struct{}{}
	s.sseMu.Unlock()
	defer func() {
		s.sseMu.Lock()
		delete(s.sseClients, ch)
		s.sseMu.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send a heartbeat comment every 15s to keep the connection alive.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) broadcast(resource, eventType, name, namespace string) {
	msg := map[string]string{
		"resource":  resource,
		"eventType": eventType,
		"name":      name,
		"namespace": namespace,
	}
	data, _ := json.Marshal(msg)
	s.sseMu.Lock()
	defer s.sseMu.Unlock()
	for ch := range s.sseClients {
		select {
		case ch <- data:
		default: // drop if client is slow
		}
	}
}

// --------------------------------------------------------------------------
// SQLite history
// --------------------------------------------------------------------------

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS dag_runs (
			id              TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			namespace       TEXT NOT NULL,
			phase           TEXT NOT NULL,
			makespan        REAL,
			start_time      TEXT,
			completion_time TEXT,
			created_at      TEXT DEFAULT (datetime('now'))
		)
	`)
	return db, err
}

func (s *Server) recordHistory(obj *unstructured.Unstructured) {
	id := string(obj.GetUID()) + "-" + obj.GetResourceVersion()
	name := obj.GetName()
	namespace := obj.GetNamespace()
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	makespan := nestedFloat(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")

	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO dag_runs (id, name, namespace, phase, makespan, start_time, completion_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, name, namespace, phase, makespan, startTime, completionTime,
	)
	if err != nil {
		log.Printf("[ui-server] error recording history for %s/%s: %v", namespace, name, err)
	}
}

type historyEntry struct {
	RunID          string  `json:"runId"`
	Phase          string  `json:"phase"`
	Makespan       float64 `json:"makespan"`
	StartTime      string  `json:"startTime"`
	CompletionTime string  `json:"completionTime"`
}

func (s *Server) queryHistory(namespace, name string) ([]historyEntry, error) {
	rows, err := s.db.Query(
		`SELECT id, phase, COALESCE(makespan, 0), COALESCE(start_time, ''), COALESCE(completion_time, '')
		 FROM dag_runs WHERE namespace = ? AND name = ? ORDER BY created_at DESC LIMIT 50`,
		namespace, name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []historyEntry
	for rows.Next() {
		var e historyEntry
		if err := rows.Scan(&e.RunID, &e.Phase, &e.Makespan, &e.StartTime, &e.CompletionTime); err != nil {
			continue
		}
		result = append(result, e)
	}
	if result == nil {
		result = []historyEntry{}
	}
	return result, nil
}

// --------------------------------------------------------------------------
// Response builders
// --------------------------------------------------------------------------

func odagSummary(obj *unstructured.Unstructured) map[string]interface{} {
	tasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	sched, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	makespan := nestedFloat(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
	return map[string]interface{}{
		"name":           obj.GetName(),
		"namespace":      obj.GetNamespace(),
		"phase":          defaultStr(phase, "Pending"),
		"scheduler":      defaultStr(sched, "random"),
		"taskCount":      len(tasks),
		"makespan":       makespan,
		"startTime":      startTime,
		"completionTime": completionTime,
		"createdAt":      obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
	}
}

func odagDetail(obj *unstructured.Unstructured) map[string]interface{} {
	summary := odagSummary(obj)
	taskStatuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	predictedTasks, _, _ := unstructured.NestedSlice(obj.Object, "status", "predictedTasks")
	summary["tasks"] = taskStatuses
	summary["spec"] = map[string]interface{}{"tasks": specTasks}
	if predictedTasks != nil {
		summary["predictedTasks"] = predictedTasks
	}
	return summary
}

func cdagSummary(obj *unstructured.Unstructured) map[string]interface{} {
	tasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	sched, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	return map[string]interface{}{
		"name":      obj.GetName(),
		"namespace": obj.GetNamespace(),
		"phase":     defaultStr(phase, "Pending"),
		"scheduler": defaultStr(sched, "random"),
		"taskCount": len(tasks),
		"createdAt": obj.GetCreationTimestamp().UTC().Format(time.RFC3339),
	}
}

func cdagDetail(obj *unstructured.Unstructured) map[string]interface{} {
	summary := cdagSummary(obj)
	taskStatuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	summary["tasks"] = taskStatuses
	summary["spec"] = map[string]interface{}{"tasks": specTasks}
	return summary
}

// --------------------------------------------------------------------------
// Utilities

// nestedFloat reads a status numeric field that may be stored as int64 or
// float64 depending on how the API server round-trips the CRD value.
func nestedFloat(obj map[string]interface{}, fields ...string) float64 {
	val, found, _ := unstructured.NestedFieldNoCopy(obj, fields...)
	if !found || val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	}
	return 0
}

// --------------------------------------------------------------------------

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	return cfg, nil
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[ui-server] error encoding response: %v", err)
	}
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func defaultStr(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
