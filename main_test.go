package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	tests := map[string]int64{
		"64M": 64 * 1024 * 1024,
		"1G":  1024 * 1024 * 1024,
		"10":  10,
		"2MB": 2 * 1000 * 1000,
	}
	for input, want := range tests {
		got, err := parseSize(input)
		if err != nil {
			t.Fatalf("parseSize(%q) returned error: %v", input, err)
		}
		if got != want {
			t.Fatalf("parseSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestAppDefaultModeFromExecutableName(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{filepath.Join("tmp", "Data Hoarder File Transfer (DHFT)-Server-Windows.exe")}
	if got := appDefaultMode(); got != "server" {
		t.Fatalf("server executable default = %q", got)
	}
	os.Args = []string{filepath.Join("tmp", "Data Hoarder File Transfer (DHFT)-Client-macOS")}
	if got := appDefaultMode(); got != "client" {
		t.Fatalf("client executable default = %q", got)
	}
	os.Args = []string{filepath.Join("tmp", "hypercopy")}
	if got := appDefaultMode(); got != "" {
		t.Fatalf("generic executable default = %q", got)
	}
}

func TestRangesRoundTrip(t *testing.T) {
	done := []bool{true, true, false, true, false, true, true, true}
	ranges := rangesFromBools(done)
	got := boolsFromRanges(ranges, len(done))
	for i := range done {
		if got[i] != done[i] {
			t.Fatalf("index %d = %v, want %v", i, got[i], done[i])
		}
	}
}

func TestSafeJoinRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	if _, err := safeJoin(root, "../outside.txt"); err == nil {
		t.Fatal("expected parent path to be rejected")
	}
	if _, err := safeJoin(root, "/absolute.txt"); err == nil {
		t.Fatal("expected absolute path to be rejected")
	}
	if _, err := safeJoin(root, "folder/../outside.txt"); err == nil {
		t.Fatal("expected embedded parent path to be rejected")
	}
	if _, err := safeJoin(root, "bad\x00name"); err == nil {
		t.Fatal("expected null byte path to be rejected")
	}
	got, err := safeJoin(root, "folder/file.txt")
	if err != nil {
		t.Fatalf("safe path rejected: %v", err)
	}
	want := filepath.Join(root, "folder", "file.txt")
	if got != want {
		t.Fatalf("safeJoin returned %q, want %q", got, want)
	}
}

func TestLocalTransferEndToEnd(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "a.txt"), []byte("hello from dhft"))
	mustWriteFile(t, filepath.Join(source, "nested", "b.bin"), []byte(strings.Repeat("0123456789", 1000)))
	mustWriteFile(t, filepath.Join(source, "empty.dat"), nil)

	uploader := startTestUploader(t, source, "secret")
	cfg := testClientConfig(uploader.addr, dest)
	cfg.ChunkSize = 17

	ctx := context.Background()
	entries, totalBytes, totalFiles, err := fetchManifest(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if totalFiles != 3 || totalBytes == 0 {
		t.Fatalf("manifest totals = files %d bytes %d", totalFiles, totalBytes)
	}
	receiveEntries(t, cfg, entries, totalBytes)

	assertFileEqual(t, filepath.Join(source, "a.txt"), filepath.Join(dest, "a.txt"))
	assertFileEqual(t, filepath.Join(source, "nested", "b.bin"), filepath.Join(dest, "nested", "b.bin"))
	assertFileEqual(t, filepath.Join(source, "empty.dat"), filepath.Join(dest, "empty.dat"))

	sourceBytes := fileSize(t, filepath.Join(source, "a.txt")) +
		fileSize(t, filepath.Join(source, "nested", "b.bin")) +
		fileSize(t, filepath.Join(source, "empty.dat"))
	clientStats := cfg.Stats.Snapshot()
	if clientStats.NetDownload != sourceBytes {
		t.Fatalf("client download bytes = %d, want %d", clientStats.NetDownload, sourceBytes)
	}
	if clientStats.DiskWrite != sourceBytes {
		t.Fatalf("client disk write bytes = %d, want %d", clientStats.DiskWrite, sourceBytes)
	}
	if clientStats.DiskRead < sourceBytes {
		t.Fatalf("client disk read bytes = %d, want at least %d", clientStats.DiskRead, sourceBytes)
	}
	serverStats := uploader.stats.Snapshot()
	if serverStats.NetUpload != sourceBytes {
		t.Fatalf("server upload bytes = %d, want %d", serverStats.NetUpload, sourceBytes)
	}
	if serverStats.DiskRead < sourceBytes {
		t.Fatalf("server disk read bytes = %d, want at least %d", serverStats.DiskRead, sourceBytes)
	}
}

func TestManyFilesAndLargeChunkedFileTransfer(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	for i := 0; i < 120; i++ {
		rel := filepath.Join("small", fmt.Sprintf("group-%02d", i%7), fmt.Sprintf("file-%03d.txt", i))
		mustWriteFile(t, filepath.Join(source, rel), []byte(strings.Repeat(fmt.Sprintf("file-%03d\n", i), i%17+1)))
	}
	large := bytes.Repeat([]byte("large-transfer-block-0123456789\n"), 160000)
	mustWriteFile(t, filepath.Join(source, "large.bin"), large)
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, dest)
	cfg.Workers = 6
	cfg.ChunkSize = 128 * 1024
	entries, totalBytes, totalFiles, err := fetchManifest(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if totalFiles != 121 {
		t.Fatalf("manifest files = %d, want 121", totalFiles)
	}
	if totalBytes < int64(len(large)) {
		t.Fatalf("manifest bytes = %d, expected at least large file size %d", totalBytes, len(large))
	}
	receiveEntries(t, cfg, entries, totalBytes)

	assertFileEqual(t, filepath.Join(source, "large.bin"), filepath.Join(dest, "large.bin"))
	for i := 0; i < 120; i++ {
		rel := filepath.Join("small", fmt.Sprintf("group-%02d", i%7), fmt.Sprintf("file-%03d.txt", i))
		assertFileEqual(t, filepath.Join(source, rel), filepath.Join(dest, rel))
	}
}

func TestTokenAuthRejectsWrongToken(t *testing.T) {
	source := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "a.txt"), []byte("private"))
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, t.TempDir())
	cfg.Token = "wrong"
	_, _, _, err := fetchManifest(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "bad token") {
		t.Fatalf("expected bad token error, got %v", err)
	}
}

func TestChunkProtocolValidation(t *testing.T) {
	source := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "file.txt"), []byte("abcdef"))
	uploader := startTestUploader(t, source, "secret")
	cfg := testClientConfig(uploader.addr, t.TempDir())

	valid := Command{Op: "chunk", Token: cfg.Token, Path: "file.txt", Offset: 1, Length: 3}
	conn, reader, err := openCommand(context.Background(), cfg, valid)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := readResponseLine(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Length != 3 {
		t.Fatalf("valid chunk response = %+v", resp)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(reader, buf); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if string(buf) != "bcd" {
		t.Fatalf("chunk bytes = %q, want bcd", string(buf))
	}

	tests := []Command{
		{Op: "chunk", Token: cfg.Token, Path: "file.txt", Offset: -1, Length: 1},
		{Op: "chunk", Token: cfg.Token, Path: "file.txt", Offset: 0, Length: -1},
		{Op: "chunk", Token: cfg.Token, Path: "../file.txt", Offset: 0, Length: 1},
		{Op: "chunk", Token: cfg.Token, Path: "file.txt", Offset: 0, Length: 99},
		{Op: "not-real", Token: cfg.Token},
	}
	for _, cmd := range tests {
		conn, reader, err := openCommand(context.Background(), cfg, cmd)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := readResponseLine(reader)
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.OK {
			t.Fatalf("expected command %+v to fail, got %+v", cmd, resp)
		}
	}
}

func TestRemoteMD5MatchesLocalMD5(t *testing.T) {
	source := t.TempDir()
	filePath := filepath.Join(source, "file.bin")
	mustWriteFile(t, filePath, []byte(strings.Repeat("hash-me", 500)))
	uploader := startTestUploader(t, source, "secret")
	cfg := testClientConfig(uploader.addr, t.TempDir())

	remote, err := requestRemoteMD5(context.Background(), cfg, "file.bin")
	if err != nil {
		t.Fatal(err)
	}
	local, _, err := fileMD5(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if remote != local {
		t.Fatalf("remote md5 %s != local md5 %s", remote, local)
	}
}

func TestResumePartialDownloadCompletesMissingChunks(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	data := []byte(strings.Repeat("abcdefghijklmnopqrstuvwxyz", 80))
	mustWriteFile(t, filepath.Join(source, "payload.bin"), data)
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, dest)
	cfg.ChunkSize = 37
	entries := fetchTestManifest(t, cfg)
	entry := findEntry(t, entries, "payload.bin")

	partPath := filepath.Join(dest, "payload.bin.hcpart")
	writePartialChunks(t, partPath, data, cfg.ChunkSize, []int{0, 2, 5})
	state := &TransferState{
		Version:   stateVersion,
		ChunkSize: cfg.ChunkSize,
		Files: map[string]FileState{
			"payload.bin": {
				Size:    entry.Size,
				ModTime: entry.ModTime,
				Ranges:  []Range{{Start: 0, End: 1}, {Start: 2, End: 3}, {Start: 5, End: 6}},
			},
		},
	}

	progress := &Progress{total: entry.Size, start: time.Now(), paused: &atomic.Bool{}}
	if err := receiveFile(context.Background(), cfg, entry, state, progress, &atomic.Bool{}); err != nil {
		t.Fatal(err)
	}

	assertFileEqual(t, filepath.Join(source, "payload.bin"), filepath.Join(dest, "payload.bin"))
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("partial file should be gone after successful resume, stat err=%v", err)
	}
	got := state.Files["payload.bin"]
	if !got.Verified {
		t.Fatalf("resume should mark file verified, state=%+v", got)
	}
}

func TestExistingFileMD5MismatchRequiresOverwrite(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "same-size.txt"), []byte("correct-data"))
	mustWriteFile(t, filepath.Join(dest, "same-size.txt"), []byte("wrong!!-data"))
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, dest)
	entries := fetchTestManifest(t, cfg)
	entry := findEntry(t, entries, "same-size.txt")
	state := newTestState(cfg)

	progress := &Progress{total: entry.Size, start: time.Now(), paused: &atomic.Bool{}}
	err := receiveFile(context.Background(), cfg, entry, state, progress, &atomic.Bool{})
	if err == nil || !strings.Contains(err.Error(), "MD5 differs") {
		t.Fatalf("expected MD5 mismatch error, got %v", err)
	}
	assertFileContent(t, filepath.Join(dest, "same-size.txt"), []byte("wrong!!-data"))

	cfg.Overwrite = true
	if err := receiveFile(context.Background(), cfg, entry, state, progress, &atomic.Bool{}); err != nil {
		t.Fatal(err)
	}
	assertFileEqual(t, filepath.Join(source, "same-size.txt"), filepath.Join(dest, "same-size.txt"))
}

func TestSkipExistingTrustsSameSizeFile(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "skip.txt"), []byte("correct"))
	mustWriteFile(t, filepath.Join(dest, "skip.txt"), []byte("WRONG!!"))
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, dest)
	cfg.SkipExisting = true
	entries := fetchTestManifest(t, cfg)
	entry := findEntry(t, entries, "skip.txt")
	state := newTestState(cfg)

	progress := &Progress{total: entry.Size, start: time.Now(), paused: &atomic.Bool{}}
	if err := receiveFile(context.Background(), cfg, entry, state, progress, &atomic.Bool{}); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(dest, "skip.txt"), []byte("WRONG!!"))
	if !state.Files["skip.txt"].Verified {
		t.Fatalf("skip-existing should mark matching-size file verified")
	}
}

func TestCanceledTransferDoesNotCompleteWhenVerificationDisabled(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "big.bin"), []byte(strings.Repeat("0123456789", 1000)))
	uploader := startTestUploader(t, source, "secret")

	cfg := testClientConfig(uploader.addr, dest)
	cfg.Verify = "none"
	cfg.ChunkSize = 19
	entries := fetchTestManifest(t, cfg)
	entry := findEntry(t, entries, "big.bin")
	state := newTestState(cfg)
	progress := &Progress{total: entry.Size, start: time.Now(), paused: &atomic.Bool{}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := receiveFile(ctx, cfg, entry, state, progress, &atomic.Bool{})
	if err == nil {
		t.Fatal("expected canceled transfer to return an error")
	}
	if _, err := os.Stat(filepath.Join(dest, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("final file should not exist after canceled transfer, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "big.bin.hcpart")); err != nil {
		t.Fatalf("partial file should remain for resume, got %v", err)
	}
}

func TestLoadStateChunkSizeChangeClearsOnlyUnverifiedRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := &TransferState{
		Version:   stateVersion,
		ChunkSize: 64,
		Files: map[string]FileState{
			"done.bin": {
				Size:     128,
				Verified: true,
				Ranges:   []Range{{Start: 0, End: 2}},
			},
			"partial.bin": {
				Size:   128,
				Ranges: []Range{{Start: 0, End: 1}},
			},
		},
	}
	if err := saveState(path, state); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadState(path, 128)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ChunkSize != 128 {
		t.Fatalf("chunk size = %d, want 128", loaded.ChunkSize)
	}
	if got := loaded.Files["partial.bin"].Ranges; len(got) != 0 {
		t.Fatalf("unverified ranges should be cleared after chunk-size change, got %+v", got)
	}
	if got := loaded.Files["done.bin"].Ranges; len(got) != 1 {
		t.Fatalf("verified ranges should be preserved, got %+v", got)
	}
}

func TestRunClientDryRun(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "dry-run.txt"), []byte("no download"))
	uploader := startTestUploader(t, source, "secret")

	err := runClient([]string{
		"--addr", uploader.addr,
		"--dest", dest,
		"--token", "secret",
		"--dry-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dest, "dry-run.txt")); !os.IsNotExist(err) {
		t.Fatalf("dry run should not create transferred file, stat err=%v", err)
	}
}

func TestServerGUIStartStatusStop(t *testing.T) {
	source := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "gui.txt"), []byte("served from gui"))

	state := &serverGUIState{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/start":
			state.handleStart(w, r)
		case "/api/status":
			state.handleStatus(w, r)
		case "/api/stop":
			state.handleStop(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	postTestJSON(t, ts.URL+"/api/start", map[string]any{
		"dir":    source,
		"listen": "127.0.0.1:0",
		"token":  "gui-secret",
	})
	status := getServerGUIStatus(t, ts.URL+"/api/status")
	if !status.Running || status.Listen == "" {
		t.Fatalf("server GUI status = %+v", status)
	}
	cfg := testClientConfig(status.Listen, t.TempDir())
	cfg.Token = "gui-secret"
	entries := fetchTestManifest(t, cfg)
	if findEntry(t, entries, "gui.txt").Size == 0 {
		t.Fatal("expected gui.txt in manifest")
	}

	postTestJSON(t, ts.URL+"/api/stop", map[string]any{})
	status = getServerGUIStatus(t, ts.URL+"/api/status")
	if status.Running {
		t.Fatalf("server should be stopped, status=%+v", status)
	}
}

func TestClientGUIStartCompletesDownload(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	mustWriteFile(t, filepath.Join(source, "client-gui.txt"), []byte(strings.Repeat("gui download\n", 50)))
	uploader := startTestUploader(t, source, "gui-secret")

	state := &clientGUIState{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/start":
			state.handleStart(w, r)
		case "/api/status":
			state.handleStatus(w, r)
		case "/api/pause":
			state.handlePause(w, r)
		case "/api/stop":
			state.handleStop(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	postTestJSON(t, ts.URL+"/api/start", map[string]any{
		"addr":       uploader.addr,
		"dest":       dest,
		"token":      "gui-secret",
		"workers":    2,
		"chunk_size": "32K",
		"verify":     "md5",
		"retries":    2,
	})

	var status clientStatusPayload
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status = getClientGUIStatus(t, ts.URL+"/api/status")
		if !status.Running {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if status.Running || !status.Done || status.LastError != "" {
		t.Fatalf("client GUI status = %+v", status)
	}
	assertFileEqual(t, filepath.Join(source, "client-gui.txt"), filepath.Join(dest, "client-gui.txt"))
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func assertFileEqual(t *testing.T, wantPath, gotPath string) {
	t.Helper()
	want, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s did not match %s", gotPath, wantPath)
	}
}

func assertFileContent(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s content = %q, want %q", path, got, want)
	}
}

func postTestJSON(t *testing.T, url string, body any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("POST %s returned %s", url, resp.Status)
	}
}

func getServerGUIStatus(t *testing.T, url string) serverStatusPayload {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status serverStatusPayload
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func getClientGUIStatus(t *testing.T, url string) clientStatusPayload {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status clientStatusPayload
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

type testUploader struct {
	addr  string
	stats *TransferStats
}

func startTestUploader(t *testing.T, source, token string) testUploader {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	stats := &TransferStats{}
	var closed atomic.Bool
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if closed.Load() {
					return
				}
				t.Logf("accept error: %v", err)
				return
			}
			go handleServerConn(conn, ServerConfig{Root: source, Token: token, Stats: stats})
		}
	}()

	t.Cleanup(func() {
		closed.Store(true)
		_ = ln.Close()
	})
	return testUploader{addr: ln.Addr().String(), stats: stats}
}

func testClientConfig(addr, dest string) ClientConfig {
	return ClientConfig{
		Addr:        addr,
		Dest:        dest,
		Token:       "secret",
		Workers:     3,
		ChunkSize:   64,
		StatePath:   filepath.Join(dest, ".dhft-state.json"),
		Verify:      "md5",
		Retries:     2,
		IdleTimeout: 10 * time.Second,
		Stats:       &TransferStats{},
		Dashboard:   true,
	}
}

func fetchTestManifest(t *testing.T, cfg ClientConfig) []ManifestEntry {
	t.Helper()
	entries, _, _, err := fetchManifest(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func receiveEntries(t *testing.T, cfg ClientConfig, entries []ManifestEntry, totalBytes int64) {
	t.Helper()
	state, err := loadState(cfg.StatePath, cfg.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	progress := &Progress{total: totalBytes, start: time.Now(), paused: &atomic.Bool{}}
	paused := &atomic.Bool{}
	for _, entry := range entries {
		switch entry.Type {
		case "dir":
			if err := receiveDir(cfg, entry); err != nil {
				t.Fatal(err)
			}
		case "file":
			if err := receiveFile(context.Background(), cfg, entry, state, progress, paused); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func findEntry(t *testing.T, entries []ManifestEntry, rel string) ManifestEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.Path == rel {
			return entry
		}
	}
	t.Fatalf("entry %s not found in manifest", rel)
	return ManifestEntry{}
}

func newTestState(cfg ClientConfig) *TransferState {
	return &TransferState{Version: stateVersion, ChunkSize: cfg.ChunkSize, Files: map[string]FileState{}}
}

func writePartialChunks(t *testing.T, path string, data []byte, chunkSize int64, indexes []int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(int64(len(data))); err != nil {
		t.Fatal(err)
	}
	for _, index := range indexes {
		start := int64(index) * chunkSize
		if start >= int64(len(data)) {
			t.Fatalf("chunk index %d outside data length", index)
		}
		end := start + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if _, err := f.WriteAt(data[start:end], start); err != nil {
			t.Fatal(err)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
