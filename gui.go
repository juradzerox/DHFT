package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type runningTransferServer struct {
	ln   net.Listener
	cfg  ServerConfig
	done chan struct{}
	once sync.Once
}

type serverGUIState struct {
	mu        sync.Mutex
	server    *runningTransferServer
	stats     *TransferStats
	root      string
	listen    string
	startedAt time.Time
	lastError string
}

type clientGUIState struct {
	mu         sync.Mutex
	running    bool
	done       bool
	paused     *atomic.Bool
	cancel     context.CancelFunc
	progress   *Progress
	stats      *TransferStats
	startedAt  time.Time
	totalBytes int64
	totalFiles int
	lastError  string
	status     string
}

type guiStatsPayload struct {
	NetUpload   int64  `json:"net_upload"`
	NetDownload int64  `json:"net_download"`
	DiskRead    int64  `json:"disk_read"`
	DiskWrite   int64  `json:"disk_write"`
	Active      int64  `json:"active"`
	Current     string `json:"current"`
}

type serverStatusPayload struct {
	Running   bool            `json:"running"`
	Root      string          `json:"root"`
	Listen    string          `json:"listen"`
	StartedAt int64           `json:"started_at"`
	LastError string          `json:"last_error"`
	Stats     guiStatsPayload `json:"stats"`
}

type clientStatusPayload struct {
	Running    bool            `json:"running"`
	Done       bool            `json:"done"`
	Paused     bool            `json:"paused"`
	StartedAt  int64           `json:"started_at"`
	TotalBytes int64           `json:"total_bytes"`
	DoneBytes  int64           `json:"done_bytes"`
	TotalFiles int             `json:"total_files"`
	LastError  string          `json:"last_error"`
	Status     string          `json:"status"`
	Current    string          `json:"current"`
	Stats      guiStatsPayload `json:"stats"`
}

func runServerGUI(args []string) error {
	fs := flag.NewFlagSet("server-gui", flag.ExitOnError)
	guiListen := fs.String("gui-listen", "127.0.0.1:9820", "local GUI listen address")
	open := fs.Bool("open", true, "open the GUI in the default browser")
	if err := fs.Parse(args); err != nil {
		return err
	}

	state := &serverGUIState{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeHTML(w, serverGUIHTML)
	})
	mux.HandleFunc("/api/status", state.handleStatus)
	mux.HandleFunc("/api/start", state.handleStart)
	mux.HandleFunc("/api/stop", state.handleStop)
	registerCommonGUIRoutes(mux)

	return serveGUI("server", *guiListen, *open, mux)
}

func runClientGUI(args []string) error {
	fs := flag.NewFlagSet("client-gui", flag.ExitOnError)
	guiListen := fs.String("gui-listen", "127.0.0.1:9821", "local GUI listen address")
	open := fs.Bool("open", true, "open the GUI in the default browser")
	if err := fs.Parse(args); err != nil {
		return err
	}

	state := &clientGUIState{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeHTML(w, clientGUIHTML)
	})
	mux.HandleFunc("/api/status", state.handleStatus)
	mux.HandleFunc("/api/start", state.handleStart)
	mux.HandleFunc("/api/pause", state.handlePause)
	mux.HandleFunc("/api/stop", state.handleStop)
	registerCommonGUIRoutes(mux)

	return serveGUI("client", *guiListen, *open, mux)
}

func serveGUI(name, listen string, open bool, mux http.Handler) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	url := "http://" + ln.Addr().String()
	fmt.Printf("DHFT %s GUI: %s\n", name, url)
	if open {
		_ = openBrowser(url)
	}
	return http.Serve(ln, mux)
}

func registerCommonGUIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/pick-folder", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		p, err := pickFolder()
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]string{"path": p})
	})
	mux.HandleFunc("/api/quit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeAPIJSON(w, http.StatusOK, map[string]bool{"ok": true})
		go func() {
			time.Sleep(150 * time.Millisecond)
			os.Exit(0)
		}()
	})
}

func (s *serverGUIState) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	running := s.server != nil
	var stats guiStatsPayload
	if s.stats != nil {
		stats = guiStatsFromSnapshot(s.stats.Snapshot())
	}
	writeAPIJSON(w, http.StatusOK, serverStatusPayload{
		Running:   running,
		Root:      s.root,
		Listen:    s.listen,
		StartedAt: unixOrZero(s.startedAt),
		LastError: s.lastError,
		Stats:     stats,
	})
}

func (s *serverGUIState) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Dir    string `json:"dir"`
		Listen string `json:"listen"`
		Token  string `json:"token"`
	}
	if err := decodeAPIJSON(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Dir = strings.TrimSpace(req.Dir)
	req.Listen = strings.TrimSpace(req.Listen)
	if req.Listen == "" {
		req.Listen = ":" + defaultPort
	}
	if req.Dir == "" {
		writeAPIError(w, http.StatusBadRequest, "folder is required")
		return
	}
	root, err := filepath.Abs(req.Dir)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, err := os.Stat(root)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !info.IsDir() {
		writeAPIError(w, http.StatusBadRequest, "selected path is not a folder")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		writeAPIError(w, http.StatusConflict, "uploader is already running")
		return
	}

	stats := &TransferStats{}
	server, err := startRunningTransferServer(ServerConfig{
		Root:   root,
		Listen: req.Listen,
		Token:  req.Token,
		Stats:  stats,
	})
	if err != nil {
		s.lastError = err.Error()
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.server = server
	s.stats = stats
	s.root = root
	s.listen = server.ln.Addr().String()
	s.startedAt = time.Now()
	s.lastError = ""
	writeAPIJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *serverGUIState) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.mu.Lock()
	server := s.server
	s.server = nil
	s.mu.Unlock()
	if server != nil {
		_ = server.Close()
	}
	writeAPIJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *clientGUIState) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var stats guiStatsPayload
	if s.stats != nil {
		stats = guiStatsFromSnapshot(s.stats.Snapshot())
	}
	doneBytes := int64(0)
	current := ""
	if s.progress != nil {
		doneBytes = s.progress.Done()
		current = s.progress.Current()
	}
	paused := false
	if s.paused != nil {
		paused = s.paused.Load()
	}
	writeAPIJSON(w, http.StatusOK, clientStatusPayload{
		Running:    s.running,
		Done:       s.done,
		Paused:     paused,
		StartedAt:  unixOrZero(s.startedAt),
		TotalBytes: s.totalBytes,
		DoneBytes:  doneBytes,
		TotalFiles: s.totalFiles,
		LastError:  s.lastError,
		Status:     s.status,
		Current:    current,
		Stats:      stats,
	})
}

func (s *clientGUIState) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Addr         string `json:"addr"`
		Dest         string `json:"dest"`
		Token        string `json:"token"`
		Workers      int    `json:"workers"`
		ChunkSize    string `json:"chunk_size"`
		Verify       string `json:"verify"`
		Retries      int    `json:"retries"`
		Overwrite    bool   `json:"overwrite"`
		SkipExisting bool   `json:"skip_existing"`
	}
	if err := decodeAPIJSON(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Addr = strings.TrimSpace(req.Addr)
	req.Dest = strings.TrimSpace(req.Dest)
	if req.Addr == "" || req.Dest == "" {
		writeAPIError(w, http.StatusBadRequest, "server address and destination folder are required")
		return
	}
	if req.Workers <= 0 {
		req.Workers = 8
	}
	if req.ChunkSize == "" {
		req.ChunkSize = defaultChunkStr
	}
	chunkSize, err := parseSize(req.ChunkSize)
	if err != nil || chunkSize <= 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid chunk size")
		return
	}
	if req.Verify == "" {
		req.Verify = "md5"
	}
	if req.Verify != "md5" && req.Verify != "none" {
		writeAPIError(w, http.StatusBadRequest, "verification must be md5 or none")
		return
	}
	if req.Retries < 0 {
		req.Retries = 0
	}
	if req.Retries == 0 {
		req.Retries = 5
	}
	dest, err := filepath.Abs(req.Dest)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := os.MkdirAll(dest, 0755); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		writeAPIError(w, http.StatusConflict, "download is already running")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	stats := &TransferStats{}
	paused := &atomic.Bool{}
	progress := &Progress{start: time.Now(), paused: paused, stats: stats}
	cfg := ClientConfig{
		Addr:         req.Addr,
		Dest:         dest,
		Token:        req.Token,
		Workers:      req.Workers,
		ChunkSize:    chunkSize,
		StatePath:    filepath.Join(dest, ".dhft-state.json"),
		Verify:       req.Verify,
		Retries:      req.Retries,
		Overwrite:    req.Overwrite,
		SkipExisting: req.SkipExisting,
		IdleTimeout:  2 * time.Minute,
		Stats:        stats,
		Dashboard:    false,
	}
	s.running = true
	s.done = false
	s.paused = paused
	s.cancel = cancel
	s.progress = progress
	s.stats = stats
	s.startedAt = time.Now()
	s.totalBytes = 0
	s.totalFiles = 0
	s.lastError = ""
	s.status = "Connecting"
	s.mu.Unlock()

	go s.runTransfer(ctx, cfg, progress, paused)
	writeAPIJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *clientGUIState) runTransfer(ctx context.Context, cfg ClientConfig, progress *Progress, paused *atomic.Bool) {
	err := func() error {
		entries, totalBytes, totalFiles, err := fetchManifest(ctx, cfg)
		if err != nil {
			return err
		}
		progress.total = totalBytes
		s.mu.Lock()
		s.totalBytes = totalBytes
		s.totalFiles = totalFiles
		s.status = "Downloading"
		s.mu.Unlock()

		state, err := loadState(cfg.StatePath, cfg.ChunkSize)
		if err != nil {
			return err
		}
		if state.Files == nil {
			state.Files = map[string]FileState{}
		}
		for _, entry := range entries {
			if err := waitIfPaused(ctx, paused); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			switch entry.Type {
			case "dir":
				if err := receiveDir(cfg, entry); err != nil {
					return err
				}
			case "file":
				if err := receiveFile(ctx, cfg, entry, state, progress, paused); err != nil {
					_ = saveState(cfg.StatePath, state)
					return err
				}
			}
		}
		if err := saveState(cfg.StatePath, state); err != nil {
			return err
		}
		return ctx.Err()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.done = err == nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.status = "Stopped"
			s.lastError = "Stopped; press Start again to resume."
		} else {
			s.status = "Error"
			s.lastError = err.Error()
		}
	} else {
		s.status = "Complete"
		s.lastError = ""
	}
}

func (s *clientGUIState) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused == nil {
		writeAPIError(w, http.StatusConflict, "no active download")
		return
	}
	next := !s.paused.Load()
	s.paused.Store(next)
	if next {
		s.status = "Paused"
	} else if s.running {
		s.status = "Downloading"
	}
	writeAPIJSON(w, http.StatusOK, map[string]bool{"paused": next})
}

func (s *clientGUIState) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	writeAPIJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func startRunningTransferServer(cfg ServerConfig) (*runningTransferServer, error) {
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	server := &runningTransferServer{ln: ln, cfg: cfg, done: make(chan struct{})}
	go server.acceptLoop()
	return server, nil
}

func (s *runningTransferServer) acceptLoop() {
	defer close(s.done)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go handleServerConn(conn, s.cfg)
	}
}

func (s *runningTransferServer) Close() error {
	var err error
	s.once.Do(func() {
		err = s.ln.Close()
		<-s.done
	})
	return err
}

func guiStatsFromSnapshot(s StatsSnapshot) guiStatsPayload {
	return guiStatsPayload{
		NetUpload:   s.NetUpload,
		NetDownload: s.NetDownload,
		DiskRead:    s.DiskRead,
		DiskWrite:   s.DiskWrite,
		Active:      s.Active,
		Current:     s.Current,
	}
}

func writeHTML(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

func writeAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeAPIJSON(w, status, map[string]string{"error": message})
}

func decodeAPIJSON(r *http.Request, value any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(value)
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	default:
		return exec.Command("xdg-open", url).Start()
	}
}

func pickFolder() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("osascript", "-e", `POSIX path of (choose folder with prompt "Select a DHFT folder")`).Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	case "windows":
		script := `[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.FolderBrowserDialog; $d.Description = "Select a DHFT folder"; if ($d.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) { Write-Output $d.SelectedPath }`
		out, err := exec.Command("powershell", "-NoProfile", "-STA", "-Command", script).Output()
		if err != nil {
			return "", err
		}
		p := strings.TrimSpace(string(out))
		if p == "" {
			return "", errors.New("no folder selected")
		}
		return p, nil
	default:
		return "", errors.New("folder picker is not available on this platform")
	}
}

func parsePositiveInt(s string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

const guiSharedCSS = `
:root {
  color-scheme: light;
  --bg: #f5f7fb;
  --panel: #ffffff;
  --text: #172033;
  --muted: #5f6b7a;
  --line: #d7deea;
  --primary: #1264a3;
  --primary-strong: #0b4f82;
  --success: #13795b;
  --danger: #b42318;
  --warn: #b54708;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: var(--bg);
  color: var(--text);
}
.shell { max-width: 1180px; margin: 0 auto; padding: 24px; }
header { display: flex; align-items: center; justify-content: space-between; gap: 18px; margin-bottom: 18px; }
h1 { margin: 0; font-size: 25px; font-weight: 720; letter-spacing: 0; }
.status { color: var(--muted); font-size: 14px; }
.layout { display: grid; grid-template-columns: minmax(320px, 420px) 1fr; gap: 16px; align-items: start; }
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 16px; box-shadow: 0 1px 2px rgba(20, 31, 50, 0.04); }
.panel h2 { margin: 0 0 14px; font-size: 16px; font-weight: 700; }
.field { margin-bottom: 12px; }
label { display: block; margin-bottom: 6px; font-size: 13px; font-weight: 650; color: #26344d; }
input, select {
  width: 100%;
  min-height: 38px;
  border: 1px solid #c7d1df;
  border-radius: 6px;
  background: #fff;
  color: var(--text);
  padding: 8px 10px;
  font-size: 14px;
}
input:focus, select:focus { border-color: var(--primary); outline: 2px solid rgba(18, 100, 163, 0.16); }
.row { display: grid; grid-template-columns: 1fr auto; gap: 8px; align-items: end; }
.two { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.checks { display: grid; gap: 8px; margin: 10px 0 14px; }
.check { display: flex; align-items: center; gap: 8px; font-size: 14px; color: #26344d; }
.check input { width: 16px; min-height: 16px; }
.actions { display: flex; flex-wrap: wrap; gap: 8px; }
button {
  border: 1px solid #b6c2d2;
  background: #fff;
  color: #172033;
  border-radius: 6px;
  min-height: 38px;
  padding: 8px 12px;
  font-size: 14px;
  font-weight: 700;
  cursor: pointer;
}
button.primary { background: var(--primary); color: #fff; border-color: var(--primary); }
button.primary:hover { background: var(--primary-strong); }
button.danger { color: var(--danger); border-color: #f0b8b2; }
button:disabled { cursor: not-allowed; opacity: 0.55; }
.metrics { display: grid; grid-template-columns: repeat(3, minmax(140px, 1fr)); gap: 10px; }
.metric { border: 1px solid var(--line); border-radius: 8px; padding: 12px; background: #fbfcff; min-height: 82px; }
.metric .label { color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }
.metric .value { margin-top: 8px; font-size: 21px; font-weight: 760; letter-spacing: 0; overflow-wrap: anywhere; }
.metric .sub { margin-top: 3px; color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }
.bar { height: 12px; border-radius: 999px; overflow: hidden; background: #dfe7f2; margin: 12px 0 8px; }
.bar > div { height: 100%; background: linear-gradient(90deg, #1264a3, #13795b); width: 0%; transition: width 180ms linear; }
.notice { margin-top: 12px; padding: 10px 12px; border-radius: 7px; border: 1px solid var(--line); background: #fbfcff; color: var(--muted); font-size: 13px; overflow-wrap: anywhere; }
.notice.error { color: var(--danger); border-color: #f0b8b2; background: #fff7f6; }
.pill { display: inline-flex; align-items: center; min-height: 28px; border-radius: 999px; border: 1px solid var(--line); padding: 4px 10px; background: #fff; color: var(--muted); font-size: 13px; font-weight: 700; }
@media (max-width: 860px) {
  .shell { padding: 16px; }
  header, .layout { display: block; }
  header .pill { margin-top: 10px; }
  .panel { margin-bottom: 14px; }
  .metrics { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
}
`

const guiSharedJS = `
let lastStats = null;
let lastAt = 0;

function $(id) { return document.getElementById(id); }

function bytes(n) {
  n = Number(n || 0);
  if (!Number.isFinite(n)) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return i === 0 ? Math.round(n) + " " + units[i] : n.toFixed(1) + " " + units[i];
}

function rate(stats, key) {
  const now = Date.now();
  if (!lastStats || !lastAt) return 0;
  const dt = Math.max(0.001, (now - lastAt) / 1000);
  return Math.max(0, (Number(stats[key] || 0) - Number(lastStats[key] || 0)) / dt);
}

function rememberStats(stats) {
  lastStats = Object.assign({}, stats);
  lastAt = Date.now();
}

async function postJSON(url, body = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

async function pickFolder(targetId) {
  try {
    const data = await postJSON("/api/pick-folder");
    if (data.path) $(targetId).value = data.path;
  } catch (err) {
    showError(err.message);
  }
}

async function quitApp() {
  await postJSON("/api/quit");
  document.body.innerHTML = "<div class='shell'><h1>DHFT closed</h1></div>";
}

function showError(message) {
  const box = $("notice");
  if (!box) return;
  box.textContent = message || "";
  box.className = message ? "notice error" : "notice";
}
`

const serverGUIHTML = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DHFT Server</title>
  <style>` + guiSharedCSS + `</style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>DHFT Server</h1>
        <div class="status" id="headline">Uploader ready</div>
      </div>
      <div class="pill" id="runningPill">Stopped</div>
    </header>
    <div class="layout">
      <section class="panel">
        <h2>Uploader</h2>
        <div class="field">
          <label for="dir">Folder to upload</label>
          <div class="row">
            <input id="dir" placeholder="D:\FilesToSend or /Volumes/Data/Files">
            <button onclick="pickFolder('dir')">Browse</button>
          </div>
        </div>
        <div class="field">
          <label for="listen">Listen address</label>
          <input id="listen" value=":9811">
        </div>
        <div class="field">
          <label for="token">Shared token</label>
          <input id="token" type="password" autocomplete="off">
        </div>
        <div class="actions">
          <button class="primary" id="startBtn" onclick="startServer()">Start</button>
          <button class="danger" id="stopBtn" onclick="stopServer()">Stop</button>
          <button onclick="quitApp()">Quit</button>
        </div>
        <div id="notice" class="notice"></div>
      </section>
      <section class="panel">
        <h2>Live Transfer</h2>
        <div class="metrics">
          <div class="metric"><div class="label">Upload</div><div class="value" id="upRate">0 B/s</div><div class="sub" id="upTotal">0 B total</div></div>
          <div class="metric"><div class="label">Download</div><div class="value" id="downRate">0 B/s</div><div class="sub" id="downTotal">0 B total</div></div>
          <div class="metric"><div class="label">Disk Read</div><div class="value" id="readRate">0 B/s</div><div class="sub" id="readTotal">0 B total</div></div>
          <div class="metric"><div class="label">Disk Write</div><div class="value" id="writeRate">0 B/s</div><div class="sub" id="writeTotal">0 B total</div></div>
          <div class="metric"><div class="label">Active</div><div class="value" id="active">0</div><div class="sub">chunk requests</div></div>
          <div class="metric"><div class="label">Listening</div><div class="value" id="listenValue">-</div><div class="sub" id="rootValue">No folder selected</div></div>
        </div>
        <div class="notice" id="activity">No active file</div>
      </section>
    </div>
  </div>
  <script>` + guiSharedJS + `
async function startServer() {
  showError("");
  try {
    await postJSON("/api/start", {
      dir: $("dir").value,
      listen: $("listen").value,
      token: $("token").value
    });
    await refresh();
  } catch (err) { showError(err.message); }
}
async function stopServer() {
  showError("");
  try { await postJSON("/api/stop"); await refresh(); } catch (err) { showError(err.message); }
}
async function refresh() {
  const res = await fetch("/api/status");
  const data = await res.json();
  const stats = data.stats || {};
  $("runningPill").textContent = data.running ? "Running" : "Stopped";
  $("headline").textContent = data.running ? "Serving " + (data.root || "") : "Uploader ready";
  $("startBtn").disabled = !!data.running;
  $("stopBtn").disabled = !data.running;
  $("upRate").textContent = bytes(rate(stats, "net_upload")) + "/s";
  $("downRate").textContent = bytes(rate(stats, "net_download")) + "/s";
  $("readRate").textContent = bytes(rate(stats, "disk_read")) + "/s";
  $("writeRate").textContent = bytes(rate(stats, "disk_write")) + "/s";
  $("upTotal").textContent = bytes(stats.net_upload) + " total";
  $("downTotal").textContent = bytes(stats.net_download) + " total";
  $("readTotal").textContent = bytes(stats.disk_read) + " total";
  $("writeTotal").textContent = bytes(stats.disk_write) + " total";
  $("active").textContent = stats.active || 0;
  $("listenValue").textContent = data.listen || "-";
  $("rootValue").textContent = data.root || "No folder selected";
  $("activity").textContent = stats.current || "No active file";
  if (data.last_error) showError(data.last_error);
  rememberStats(stats);
}
setInterval(refresh, 1000);
refresh();
  </script>
</body>
</html>`

const clientGUIHTML = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DHFT Client</title>
  <style>` + guiSharedCSS + `</style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>DHFT Client</h1>
        <div class="status" id="headline">Downloader ready</div>
      </div>
      <div class="pill" id="runningPill">Stopped</div>
    </header>
    <div class="layout">
      <section class="panel">
        <h2>Downloader</h2>
        <div class="field">
          <label for="addr">Server address</label>
          <input id="addr" placeholder="WINDOWS_IP:9811">
        </div>
        <div class="field">
          <label for="dest">Destination folder</label>
          <div class="row">
            <input id="dest" placeholder="/Volumes/BigDisk/ReceivedFiles">
            <button onclick="pickFolder('dest')">Browse</button>
          </div>
        </div>
        <div class="field">
          <label for="token">Shared token</label>
          <input id="token" type="password" autocomplete="off">
        </div>
        <div class="two">
          <div class="field"><label for="workers">Workers</label><input id="workers" value="12"></div>
          <div class="field"><label for="chunk">Chunk size</label><input id="chunk" value="128M"></div>
        </div>
        <div class="two">
          <div class="field"><label for="verify">Verify</label><select id="verify"><option value="md5">MD5</option><option value="none">None</option></select></div>
          <div class="field"><label for="retries">Retries</label><input id="retries" value="5"></div>
        </div>
        <div class="checks">
          <label class="check"><input id="overwrite" type="checkbox">Overwrite mismatched files</label>
          <label class="check"><input id="skipExisting" type="checkbox">Skip same-size existing files</label>
        </div>
        <div class="actions">
          <button class="primary" id="startBtn" onclick="startClient()">Start</button>
          <button id="pauseBtn" onclick="pauseClient()">Pause</button>
          <button class="danger" id="stopBtn" onclick="stopClient()">Stop</button>
          <button onclick="quitApp()">Quit</button>
        </div>
        <div id="notice" class="notice"></div>
      </section>
      <section class="panel">
        <h2>Live Transfer</h2>
        <div class="bar"><div id="barFill"></div></div>
        <div class="status" id="progressText">0 B / 0 B</div>
        <div class="metrics" style="margin-top: 12px;">
          <div class="metric"><div class="label">Download</div><div class="value" id="downRate">0 B/s</div><div class="sub" id="downTotal">0 B total</div></div>
          <div class="metric"><div class="label">Upload</div><div class="value" id="upRate">0 B/s</div><div class="sub" id="upTotal">0 B total</div></div>
          <div class="metric"><div class="label">Disk Write</div><div class="value" id="writeRate">0 B/s</div><div class="sub" id="writeTotal">0 B total</div></div>
          <div class="metric"><div class="label">Disk Read</div><div class="value" id="readRate">0 B/s</div><div class="sub" id="readTotal">0 B total</div></div>
          <div class="metric"><div class="label">Files</div><div class="value" id="files">0</div><div class="sub">from manifest</div></div>
          <div class="metric"><div class="label">Active</div><div class="value" id="active">0</div><div class="sub">chunk requests</div></div>
        </div>
        <div class="notice" id="activity">No active file</div>
      </section>
    </div>
  </div>
  <script>` + guiSharedJS + `
async function startClient() {
  showError("");
  try {
    await postJSON("/api/start", {
      addr: $("addr").value,
      dest: $("dest").value,
      token: $("token").value,
      workers: Number($("workers").value || 12),
      chunk_size: $("chunk").value,
      verify: $("verify").value,
      retries: Number($("retries").value || 5),
      overwrite: $("overwrite").checked,
      skip_existing: $("skipExisting").checked
    });
    await refresh();
  } catch (err) { showError(err.message); }
}
async function pauseClient() {
  showError("");
  try { await postJSON("/api/pause"); await refresh(); } catch (err) { showError(err.message); }
}
async function stopClient() {
  showError("");
  try { await postJSON("/api/stop"); await refresh(); } catch (err) { showError(err.message); }
}
async function refresh() {
  const res = await fetch("/api/status");
  const data = await res.json();
  const stats = data.stats || {};
  const total = Number(data.total_bytes || 0);
  const done = Number(data.done_bytes || 0);
  const pct = total > 0 ? Math.min(100, (done / total) * 100) : 0;
  $("runningPill").textContent = data.running ? (data.paused ? "Paused" : "Running") : (data.done ? "Complete" : "Stopped");
  $("headline").textContent = data.status || "Downloader ready";
  $("startBtn").disabled = !!data.running;
  $("pauseBtn").disabled = !data.running;
  $("pauseBtn").textContent = data.paused ? "Resume" : "Pause";
  $("stopBtn").disabled = !data.running;
  $("barFill").style.width = pct.toFixed(1) + "%";
  $("progressText").textContent = bytes(done) + " / " + bytes(total) + "  " + pct.toFixed(1) + "%";
  $("downRate").textContent = bytes(rate(stats, "net_download")) + "/s";
  $("upRate").textContent = bytes(rate(stats, "net_upload")) + "/s";
  $("writeRate").textContent = bytes(rate(stats, "disk_write")) + "/s";
  $("readRate").textContent = bytes(rate(stats, "disk_read")) + "/s";
  $("downTotal").textContent = bytes(stats.net_download) + " total";
  $("upTotal").textContent = bytes(stats.net_upload) + " total";
  $("writeTotal").textContent = bytes(stats.disk_write) + " total";
  $("readTotal").textContent = bytes(stats.disk_read) + " total";
  $("files").textContent = data.total_files || 0;
  $("active").textContent = stats.active || 0;
  $("activity").textContent = stats.current || data.current || "No active file";
  showError(data.last_error || "");
  rememberStats(stats);
}
setInterval(refresh, 1000);
refresh();
  </script>
</body>
</html>`
