package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	protocolName    = "dhft/1"
	stateVersion    = 1
	defaultPort     = "9811"
	defaultChunkStr = "64M"
)

type Command struct {
	Op     string `json:"op"`
	Token  string `json:"token,omitempty"`
	Path   string `json:"path,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Length int64  `json:"length,omitempty"`
}

type Response struct {
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Protocol string `json:"protocol,omitempty"`
	Root     string `json:"root,omitempty"`
	Length   int64  `json:"length,omitempty"`
	MD5      string `json:"md5,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Done     bool   `json:"done,omitempty"`
	Files    int    `json:"files,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
}

type ManifestEntry struct {
	Path    string `json:"path"`
	Type    string `json:"type"`
	Size    int64  `json:"size,omitempty"`
	Mode    uint32 `json:"mode,omitempty"`
	ModTime int64  `json:"mod_time"`
}

type Range struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type FileState struct {
	Size     int64   `json:"size"`
	ModTime  int64   `json:"mod_time"`
	Verified bool    `json:"verified,omitempty"`
	Ranges   []Range `json:"ranges,omitempty"`
}

type TransferState struct {
	Version   int                  `json:"version"`
	ChunkSize int64                `json:"chunk_size"`
	Files     map[string]FileState `json:"files"`
}

type ServerConfig struct {
	Root      string
	Listen    string
	Token     string
	Stats     *TransferStats
	Dashboard bool
}

type ClientConfig struct {
	Addr         string
	Dest         string
	Token        string
	Workers      int
	ChunkSize    int64
	StatePath    string
	Verify       string
	Retries      int
	Overwrite    bool
	SkipExisting bool
	DryRun       bool
	IdleTimeout  time.Duration
	Stats        *TransferStats
	Dashboard    bool
}

type ChunkJob struct {
	Path   string
	Index  int
	Offset int64
	Length int64
}

type ChunkResult struct {
	Job ChunkJob
	Err error
}

type Progress struct {
	total         int64
	done          atomic.Int64
	start         time.Time
	currentMu     sync.Mutex
	current       string
	paused        *atomic.Bool
	stats         *TransferStats
	lastAt        time.Time
	lastUpload    int64
	lastDownload  int64
	lastDiskRead  int64
	lastDiskWrite int64
	lastLineLen   int
}

type TransferStats struct {
	netUpload   atomic.Int64
	netDownload atomic.Int64
	diskRead    atomic.Int64
	diskWrite   atomic.Int64
	active      atomic.Int64
	currentMu   sync.Mutex
	current     string
}

type StatsSnapshot struct {
	NetUpload   int64
	NetDownload int64
	DiskRead    int64
	DiskWrite   int64
	Active      int64
	Current     string
}

type ServerDashboard struct {
	stats         *TransferStats
	start         time.Time
	lastAt        time.Time
	lastUpload    int64
	lastDownload  int64
	lastDiskRead  int64
	lastDiskWrite int64
	lastLineLen   int
}

func main() {
	if len(os.Args) < 2 {
		var err error
		switch appDefaultMode() {
		case "server":
			err = runServerGUI(nil)
		case "client":
			err = runClientGUI(nil)
		default:
			printUsage()
			os.Exit(2)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
			os.Exit(1)
		}
		return
	}

	var err error
	switch os.Args[1] {
	case "serve", "server":
		err = runServer(os.Args[2:])
	case "serve-gui", "server-gui":
		err = runServerGUI(os.Args[2:])
	case "receive", "client":
		err = runClient(os.Args[2:])
	case "receive-gui", "client-gui":
		err = runClientGUI(os.Args[2:])
	case "version":
		fmt.Printf("DHFT %s (%s/%s)\n", protocolName, runtime.GOOS, runtime.GOARCH)
	default:
		printUsage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func appDefaultMode() string {
	name := strings.ToLower(filepath.Base(os.Args[0]))
	switch {
	case strings.Contains(name, "server"), strings.Contains(name, "uploader"):
		return "server"
	case strings.Contains(name, "client"), strings.Contains(name, "receiver"), strings.Contains(name, "downloader"):
		return "client"
	default:
		return ""
	}
}

func printUsage() {
	fmt.Print(`Data Hoarder File Transfer (DHFT) - direct peer-to-peer file transfer

Usage:
  dhft serve   --dir <folder> --listen :9811 [--token secret]
  dhft receive --addr <host:9811> --dest <folder> [--token secret]
  dhft server-gui
  dhft client-gui

Commands:
  serve, server     Run on the uploader machine.
  receive, client   Run on the downloader machine.
  server-gui        Open the uploader GUI.
  client-gui        Open the downloader GUI.
  version           Print build information.
`)
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dir := fs.String("dir", ".", "folder to serve")
	listen := fs.String("listen", ":"+defaultPort, "listen address")
	token := fs.String("token", "", "optional shared token")
	dashboard := fs.Bool("dashboard", true, "show live speed dashboard")
	if err := fs.Parse(args); err != nil {
		return err
	}

	root, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a folder", root)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("DHFT uploader")
	fmt.Println("Serving:", root)
	fmt.Println("Listening:", *listen)
	if *token == "" {
		fmt.Println("Token auth: disabled")
	} else {
		fmt.Println("Token auth: enabled")
	}
	fmt.Println("Keep this window open while the Mac downloads.")

	stats := &TransferStats{}
	cfg := ServerConfig{Root: root, Listen: *listen, Token: *token, Stats: stats, Dashboard: *dashboard}
	if cfg.Dashboard {
		go (&ServerDashboard{stats: stats, start: time.Now()}).Run()
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleServerConn(conn, cfg)
	}
}

func handleServerConn(conn net.Conn, cfg ServerConfig) {
	defer conn.Close()
	prepareTCP(conn)

	reader := bufio.NewReaderSize(conn, 1024*1024)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	cfg.Stats.AddDownload(int64(len(line)))

	var cmd Command
	if err := json.Unmarshal(line, &cmd); err != nil {
		writeJSON(conn, Response{OK: false, Error: "invalid command"})
		return
	}
	if cfg.Token != "" && cmd.Token != cfg.Token {
		writeJSON(conn, Response{OK: false, Error: "bad token"})
		return
	}

	switch cmd.Op {
	case "manifest":
		serveManifest(conn, cfg.Root)
	case "chunk":
		serveChunk(conn, cfg.Root, cmd, cfg.Stats)
	case "md5":
		serveMD5(conn, cfg.Root, cmd, cfg.Stats)
	default:
		writeJSON(conn, Response{OK: false, Error: "unknown operation"})
	}
}

func serveManifest(w io.Writer, root string) {
	if err := writeJSON(w, Response{OK: true, Protocol: protocolName, Root: filepath.Base(root)}); err != nil {
		return
	}

	enc := json.NewEncoder(w)
	var files int
	var bytes int64

	err := filepath.WalkDir(root, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			fmt.Fprintln(os.Stderr, "Skipping:", p, walkErr)
			return nil
		}
		if p == root {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Skipping:", p, err)
			return nil
		}

		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		mode := info.Mode()
		if mode&os.ModeSymlink != 0 {
			fmt.Fprintln(os.Stderr, "Skipping symlink:", rel)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		switch {
		case d.IsDir():
			_ = enc.Encode(ManifestEntry{
				Path:    rel,
				Type:    "dir",
				Mode:    uint32(mode.Perm()),
				ModTime: info.ModTime().UnixNano(),
			})
		case mode.IsRegular():
			files++
			bytes += info.Size()
			_ = enc.Encode(ManifestEntry{
				Path:    rel,
				Type:    "file",
				Size:    info.Size(),
				Mode:    uint32(mode.Perm()),
				ModTime: info.ModTime().UnixNano(),
			})
		default:
			fmt.Fprintln(os.Stderr, "Skipping special file:", rel)
		}
		return nil
	})

	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error(), Done: true})
		return
	}
	_ = writeJSON(w, Response{OK: true, Done: true, Files: files, Bytes: bytes})
}

func serveChunk(w io.Writer, root string, cmd Command, stats *TransferStats) {
	if cmd.Offset < 0 || cmd.Length < 0 {
		_ = writeJSON(w, Response{OK: false, Error: "bad chunk range"})
		return
	}
	full, err := safeJoin(root, cmd.Path)
	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}

	f, err := os.Open(full)
	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}
	if !info.Mode().IsRegular() {
		_ = writeJSON(w, Response{OK: false, Error: "not a regular file"})
		return
	}
	if cmd.Offset > info.Size() || cmd.Length > info.Size()-cmd.Offset {
		_ = writeJSON(w, Response{OK: false, Error: "chunk outside file"})
		return
	}

	if _, err := f.Seek(cmd.Offset, io.SeekStart); err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}
	if err := writeJSON(w, Response{OK: true, Length: cmd.Length}); err != nil {
		return
	}
	done := stats.Begin("uploading " + cmd.Path)
	defer done()
	_ = copyFileRangeToNetwork(w, f, cmd.Length, stats)
}

func serveMD5(w io.Writer, root string, cmd Command, stats *TransferStats) {
	full, err := safeJoin(root, cmd.Path)
	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}
	done := stats.Begin("hashing " + cmd.Path)
	defer done()
	sum, size, err := fileMD5WithStats(full, stats)
	if err != nil {
		_ = writeJSON(w, Response{OK: false, Error: err.Error()})
		return
	}
	_ = writeJSON(w, Response{OK: true, MD5: sum, Size: size})
}

func runClient(args []string) error {
	fs := flag.NewFlagSet("receive", flag.ExitOnError)
	addr := fs.String("addr", "", "uploader address, for example 203.0.113.10:9811")
	dest := fs.String("dest", ".", "destination folder")
	token := fs.String("token", "", "shared token if the uploader uses one")
	workers := fs.Int("workers", 8, "parallel chunk connections")
	chunkSizeStr := fs.String("chunk-size", defaultChunkStr, "chunk size, for example 32M, 64M, 128M")
	statePath := fs.String("state", "", "resume state file; default is <dest>/.dhft-state.json")
	verify := fs.String("verify", "md5", "verification mode: md5 or none")
	retries := fs.Int("retries", 5, "chunk retry count")
	overwrite := fs.Bool("overwrite", false, "replace conflicting destination files")
	skipExisting := fs.Bool("skip-existing", false, "skip existing final files by size without MD5")
	dryRun := fs.Bool("dry-run", false, "fetch and summarize the manifest without downloading")
	idleTimeout := fs.Duration("idle-timeout", 2*time.Minute, "network idle timeout per read")
	dashboard := fs.Bool("dashboard", true, "show live speed dashboard")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" {
		return errors.New("--addr is required")
	}
	if *workers < 1 {
		return errors.New("--workers must be at least 1")
	}
	if *verify != "md5" && *verify != "none" {
		return errors.New("--verify must be md5 or none")
	}

	chunkSize, err := parseSize(*chunkSizeStr)
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		return errors.New("--chunk-size must be greater than zero")
	}

	destAbs, err := filepath.Abs(*dest)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(destAbs, 0755); err != nil {
		return err
	}

	sp := *statePath
	if sp == "" {
		sp = filepath.Join(destAbs, ".dhft-state.json")
	}
	sp, err = filepath.Abs(sp)
	if err != nil {
		return err
	}

	stats := &TransferStats{}
	cfg := ClientConfig{
		Addr:         *addr,
		Dest:         destAbs,
		Token:        *token,
		Workers:      *workers,
		ChunkSize:    chunkSize,
		StatePath:    sp,
		Verify:       *verify,
		Retries:      *retries,
		Overwrite:    *overwrite,
		SkipExisting: *skipExisting,
		DryRun:       *dryRun,
		IdleTimeout:  *idleTimeout,
		Stats:        stats,
		Dashboard:    *dashboard,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	installInterruptHandler(cancel)

	fmt.Println("Connecting to uploader...")
	entries, totalBytes, totalFiles, err := fetchManifest(ctx, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("Manifest: %d files, %s\n", totalFiles, formatBytes(totalBytes))
	if cfg.DryRun {
		fmt.Println("Dry run complete. No files were downloaded.")
		return nil
	}

	state, err := loadState(cfg.StatePath, cfg.ChunkSize)
	if err != nil {
		return err
	}
	if state.Files == nil {
		state.Files = map[string]FileState{}
	}

	var paused atomic.Bool
	go readPauseCommands(&paused, cancel)

	progress := &Progress{total: totalBytes, start: time.Now(), paused: &paused, stats: stats}
	progress.SetCurrent("starting")
	progressCtx, stopProgress := context.WithCancel(context.Background())
	defer stopProgress()
	if cfg.Dashboard {
		go progress.Run(progressCtx)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			break
		}
		switch entry.Type {
		case "dir":
			if err := receiveDir(cfg, entry); err != nil {
				return err
			}
		case "file":
			if err := receiveFile(ctx, cfg, entry, state, progress, &paused); err != nil {
				_ = saveState(cfg.StatePath, state)
				return err
			}
		}
	}

	stopProgress()
	if cfg.Dashboard {
		progress.PrintFinal()
	}
	if err := saveState(cfg.StatePath, state); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.New("stopped; rerun the same receive command to resume")
	}
	fmt.Println("Download complete.")
	return nil
}

func installInterruptHandler(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		fmt.Println()
		fmt.Println("Stopping after current work. Rerun the same command to resume.")
		cancel()
	}()
}

func readPauseCommands(paused *atomic.Bool, cancel context.CancelFunc) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := strings.ToLower(strings.TrimSpace(scanner.Text()))
		switch cmd {
		case "p", "pause":
			next := !paused.Load()
			paused.Store(next)
			if next {
				fmt.Println()
				fmt.Println("Paused. Type p and press Enter to resume, or q to stop.")
			} else {
				fmt.Println()
				fmt.Println("Resumed.")
			}
		case "q", "quit", "stop":
			fmt.Println()
			fmt.Println("Stopping. Rerun the same command to resume.")
			cancel()
			return
		}
	}
}

func receiveDir(cfg ClientConfig, entry ManifestEntry) error {
	full, err := safeJoin(cfg.Dest, entry.Path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(full, os.FileMode(entry.Mode&0777)); err != nil {
		return err
	}
	t := time.Unix(0, entry.ModTime)
	_ = os.Chtimes(full, t, t)
	return nil
}

func receiveFile(ctx context.Context, cfg ClientConfig, entry ManifestEntry, state *TransferState, progress *Progress, paused *atomic.Bool) error {
	progress.SetCurrent(entry.Path)

	finalPath, err := safeJoin(cfg.Dest, entry.Path)
	if err != nil {
		return err
	}
	partPath := finalPath + ".hcpart"
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return err
	}

	if st, ok := state.Files[entry.Path]; ok && st.Verified && st.Size == entry.Size && sameFileSize(finalPath, entry.Size) {
		progress.Add(entry.Size)
		return nil
	}

	if sameFileSize(finalPath, entry.Size) {
		if cfg.SkipExisting || cfg.Verify == "none" {
			state.Files[entry.Path] = FileState{Size: entry.Size, ModTime: entry.ModTime, Verified: true}
			progress.Add(entry.Size)
			return saveState(cfg.StatePath, state)
		}
		ok, err := verifyExisting(ctx, cfg, finalPath, entry.Path)
		if err != nil {
			return err
		}
		if ok {
			state.Files[entry.Path] = FileState{Size: entry.Size, ModTime: entry.ModTime, Verified: true}
			progress.Add(entry.Size)
			return saveState(cfg.StatePath, state)
		}
		if !cfg.Overwrite {
			return fmt.Errorf("%s exists but MD5 differs; rerun with --overwrite to replace it", finalPath)
		}
	}

	if fileExists(finalPath) {
		if !cfg.Overwrite {
			return fmt.Errorf("%s already exists; rerun with --overwrite to replace it", finalPath)
		}
		if err := os.Remove(finalPath); err != nil {
			return err
		}
	}

	chunkCount := chunksFor(entry.Size, cfg.ChunkSize)
	done := make([]bool, chunkCount)
	if partExists(partPath) {
		if st, ok := state.Files[entry.Path]; ok && st.Size == entry.Size && st.ModTime == entry.ModTime {
			done = boolsFromRanges(st.Ranges, chunkCount)
		}
	} else {
		state.Files[entry.Path] = FileState{Size: entry.Size, ModTime: entry.ModTime}
	}

	var alreadyDone int64
	for i, ok := range done {
		if ok {
			alreadyDone += chunkLength(i, entry.Size, cfg.ChunkSize)
		}
	}
	if alreadyDone > 0 {
		progress.Add(alreadyDone)
	}

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, os.FileMode(entry.Mode&0777))
	if err != nil {
		return err
	}
	if err := f.Truncate(entry.Size); err != nil {
		_ = f.Close()
		return err
	}

	missing := make([]ChunkJob, 0)
	for i := 0; i < chunkCount; i++ {
		if done[i] {
			continue
		}
		offset := int64(i) * cfg.ChunkSize
		missing = append(missing, ChunkJob{
			Path:   entry.Path,
			Index:  i,
			Offset: offset,
			Length: chunkLength(i, entry.Size, cfg.ChunkSize),
		})
	}

	if len(missing) > 0 {
		if err := downloadChunks(ctx, cfg, f, missing, done, entry, state, progress, paused); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}

	if cfg.Verify == "md5" {
		progress.SetCurrent("verifying " + entry.Path)
		ok, err := verifyExisting(ctx, cfg, partPath, entry.Path)
		if err != nil {
			return err
		}
		if !ok {
			state.Files[entry.Path] = FileState{Size: entry.Size, ModTime: entry.ModTime}
			_ = saveState(cfg.StatePath, state)
			return fmt.Errorf("MD5 mismatch for %s; partial file kept for inspection: %s", entry.Path, partPath)
		}
	}

	t := time.Unix(0, entry.ModTime)
	_ = os.Chtimes(partPath, t, t)
	if err := os.Rename(partPath, finalPath); err != nil {
		return err
	}
	state.Files[entry.Path] = FileState{Size: entry.Size, ModTime: entry.ModTime, Verified: true}
	return saveState(cfg.StatePath, state)
}

func downloadChunks(ctx context.Context, cfg ClientConfig, f *os.File, missing []ChunkJob, done []bool, entry ManifestEntry, state *TransferState, progress *Progress, paused *atomic.Bool) error {
	jobs := make(chan ChunkJob)
	results := make(chan ChunkResult)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := waitIfPaused(workerCtx, paused); err != nil {
					results <- ChunkResult{Job: job, Err: err}
					continue
				}
				err := downloadChunkWithRetry(workerCtx, cfg, f, job)
				results <- ChunkResult{Job: job, Err: err}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, job := range missing {
			if err := waitIfPaused(workerCtx, paused); err != nil {
				return
			}
			select {
			case <-workerCtx.Done():
				return
			case jobs <- job:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	completedSinceSave := 0
	for result := range results {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
				cancel()
			}
			continue
		}
		if !done[result.Job.Index] {
			done[result.Job.Index] = true
			progress.Add(result.Job.Length)
			completedSinceSave++
		}
		if completedSinceSave >= 32 {
			state.Files[entry.Path] = FileState{
				Size:    entry.Size,
				ModTime: entry.ModTime,
				Ranges:  rangesFromBools(done),
			}
			_ = saveState(cfg.StatePath, state)
			completedSinceSave = 0
		}
	}
	state.Files[entry.Path] = FileState{
		Size:    entry.Size,
		ModTime: entry.ModTime,
		Ranges:  rangesFromBools(done),
	}
	_ = saveState(cfg.StatePath, state)
	if firstErr == nil {
		for _, ok := range done {
			if !ok {
				if err := workerCtx.Err(); err != nil {
					firstErr = err
				} else {
					firstErr = errors.New("download incomplete")
				}
				break
			}
		}
	}
	return firstErr
}

func waitIfPaused(ctx context.Context, paused *atomic.Bool) error {
	for paused.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return ctx.Err()
}

func downloadChunkWithRetry(ctx context.Context, cfg ClientConfig, f *os.File, job ChunkJob) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.Retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			backoff := time.Duration(attempt) * 750 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		if err := downloadChunk(ctx, cfg, f, job); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%s chunk %d failed after %d retries: %w", job.Path, job.Index, cfg.Retries, lastErr)
}

func downloadChunk(ctx context.Context, cfg ClientConfig, f *os.File, job ChunkJob) error {
	done := cfg.Stats.Begin("downloading " + job.Path)
	defer done()

	conn, reader, err := openCommand(ctx, cfg, Command{
		Op:     "chunk",
		Token:  cfg.Token,
		Path:   job.Path,
		Offset: job.Offset,
		Length: job.Length,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := readResponseLine(reader)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if resp.Length != job.Length {
		return fmt.Errorf("server sent unexpected chunk length for %s", job.Path)
	}

	buf := make([]byte, 1024*1024)
	var written int64
	for written < job.Length {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(cfg.IdleTimeout))
		want := int64(len(buf))
		remaining := job.Length - written
		if remaining < want {
			want = remaining
		}
		n, readErr := io.ReadFull(reader, buf[:want])
		if n > 0 {
			cfg.Stats.AddDownload(int64(n))
			if _, err := f.WriteAt(buf[:n], job.Offset+written); err != nil {
				return err
			}
			cfg.Stats.AddDiskWrite(int64(n))
			written += int64(n)
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func verifyExisting(ctx context.Context, cfg ClientConfig, localPath, remoteRel string) (bool, error) {
	var localHash string
	var localErr error
	var remoteHash string
	var remoteErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		done := cfg.Stats.Begin("hashing local file")
		defer done()
		localHash, _, localErr = fileMD5WithStats(localPath, cfg.Stats)
	}()
	go func() {
		defer wg.Done()
		remoteHash, remoteErr = requestRemoteMD5(ctx, cfg, remoteRel)
	}()
	wg.Wait()
	if localErr != nil {
		return false, localErr
	}
	if remoteErr != nil {
		return false, remoteErr
	}
	return strings.EqualFold(localHash, remoteHash), nil
}

func requestRemoteMD5(ctx context.Context, cfg ClientConfig, rel string) (string, error) {
	conn, reader, err := openCommand(ctx, cfg, Command{Op: "md5", Token: cfg.Token, Path: rel})
	if err != nil {
		return "", err
	}
	defer conn.Close()

	resp, err := readResponseLine(reader)
	if err != nil {
		return "", err
	}
	if !resp.OK {
		return "", errors.New(resp.Error)
	}
	return resp.MD5, nil
}

func fetchManifest(ctx context.Context, cfg ClientConfig) ([]ManifestEntry, int64, int, error) {
	conn, reader, err := openCommand(ctx, cfg, Command{Op: "manifest", Token: cfg.Token})
	if err != nil {
		return nil, 0, 0, err
	}
	defer conn.Close()

	resp, err := readResponseLine(reader)
	if err != nil {
		return nil, 0, 0, err
	}
	if !resp.OK {
		return nil, 0, 0, errors.New(resp.Error)
	}

	entries := make([]ManifestEntry, 0, 1024)
	var totalBytes int64
	var files int

	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, 0, 0, err
		}

		var maybeDone Response
		if err := json.Unmarshal(line, &maybeDone); err == nil && maybeDone.Done {
			if !maybeDone.OK {
				return nil, 0, 0, errors.New(maybeDone.Error)
			}
			if maybeDone.Bytes > 0 {
				totalBytes = maybeDone.Bytes
			}
			if maybeDone.Files > 0 {
				files = maybeDone.Files
			}
			break
		}

		var entry ManifestEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, 0, 0, err
		}
		entries = append(entries, entry)
		if entry.Type == "file" {
			totalBytes += entry.Size
			files++
		}
	}
	return entries, totalBytes, files, nil
}

func openCommand(ctx context.Context, cfg ClientConfig, cmd Command) (net.Conn, *bufio.Reader, error) {
	d := net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, nil, err
	}
	prepareTCP(conn)
	_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	b, err := json.Marshal(cmd)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	n, err := conn.Write(append(b, '\n'))
	if n > 0 {
		cfg.Stats.AddUpload(int64(n))
	}
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	_ = conn.SetWriteDeadline(time.Time{})
	return conn, bufio.NewReaderSize(conn, 1024*1024), nil
}

func readResponseLine(reader *bufio.Reader) (Response, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return Response{}, err
	}
	return resp, nil
}

func writeJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

func fileMD5(p string) (string, int64, error) {
	return fileMD5WithStats(p, nil)
}

func fileMD5WithStats(p string, stats *TransferStats) (string, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	var h hash.Hash = md5.New()
	buf := make([]byte, 4*1024*1024)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			stats.AddDiskRead(int64(n))
			if _, err := h.Write(buf[:n]); err != nil {
				return "", 0, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", 0, readErr
		}
	}
	return hex.EncodeToString(h.Sum(nil)), info.Size(), nil
}

func copyFileRangeToNetwork(w io.Writer, f *os.File, length int64, stats *TransferStats) error {
	buf := make([]byte, 1024*1024)
	remaining := length
	for remaining > 0 {
		want := int64(len(buf))
		if remaining < want {
			want = remaining
		}
		n, readErr := f.Read(buf[:want])
		if n > 0 {
			stats.AddDiskRead(int64(n))
			written, writeErr := w.Write(buf[:n])
			if written > 0 {
				stats.AddUpload(int64(written))
			}
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			remaining -= int64(n)
		}
		if readErr == io.EOF && remaining == 0 {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func safeJoin(root, relSlash string) (string, error) {
	if relSlash == "" {
		return "", errors.New("empty path")
	}
	if strings.Contains(relSlash, "\x00") {
		return "", errors.New("path contains null byte")
	}
	slashPath := filepath.ToSlash(relSlash)
	if path.IsAbs(slashPath) || filepath.IsAbs(relSlash) {
		return "", errors.New("absolute paths are not allowed")
	}
	for _, part := range strings.Split(slashPath, "/") {
		if part == ".." {
			return "", errors.New("parent path segments are not allowed")
		}
	}
	clean := path.Clean("/" + slashPath)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." || clean == "" || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", errors.New("invalid relative path")
	}

	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	full := filepath.Join(rootAbs, filepath.FromSlash(clean))
	fullAbs, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, fullAbs)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.New("path escapes root")
	}
	return fullAbs, nil
}

func loadState(path string, chunkSize int64) (*TransferState, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return &TransferState{Version: stateVersion, ChunkSize: chunkSize, Files: map[string]FileState{}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var state TransferState
	if err := json.NewDecoder(f).Decode(&state); err != nil {
		return nil, err
	}
	if state.Files == nil {
		state.Files = map[string]FileState{}
	}
	if state.Version != stateVersion {
		return nil, fmt.Errorf("unsupported state version %d", state.Version)
	}
	if state.ChunkSize != chunkSize {
		for path, st := range state.Files {
			if !st.Verified {
				st.Ranges = nil
				state.Files[path] = st
			}
		}
		state.ChunkSize = chunkSize
	}
	return &state, nil
}

func saveState(path string, state *TransferState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(state); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func rangesFromBools(done []bool) []Range {
	ranges := make([]Range, 0)
	for i := 0; i < len(done); {
		if !done[i] {
			i++
			continue
		}
		start := i
		for i < len(done) && done[i] {
			i++
		}
		ranges = append(ranges, Range{Start: start, End: i})
	}
	return ranges
}

func boolsFromRanges(ranges []Range, count int) []bool {
	done := make([]bool, count)
	for _, r := range ranges {
		start := r.Start
		end := r.End
		if start < 0 {
			start = 0
		}
		if end > count {
			end = count
		}
		for i := start; i < end; i++ {
			done[i] = true
		}
	}
	return done
}

func chunksFor(size, chunkSize int64) int {
	if size == 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

func chunkLength(index int, fileSize, chunkSize int64) int64 {
	offset := int64(index) * chunkSize
	remaining := fileSize - offset
	if remaining < chunkSize {
		return remaining
	}
	return chunkSize
}

func parseSize(s string) (int64, error) {
	value := strings.TrimSpace(strings.ToUpper(s))
	if value == "" {
		return 0, errors.New("empty size")
	}
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(value, "KB"):
		multiplier = 1000
		value = strings.TrimSuffix(value, "KB")
	case strings.HasSuffix(value, "K"):
		multiplier = 1024
		value = strings.TrimSuffix(value, "K")
	case strings.HasSuffix(value, "MB"):
		multiplier = 1000 * 1000
		value = strings.TrimSuffix(value, "MB")
	case strings.HasSuffix(value, "M"):
		multiplier = 1024 * 1024
		value = strings.TrimSuffix(value, "M")
	case strings.HasSuffix(value, "GB"):
		multiplier = 1000 * 1000 * 1000
		value = strings.TrimSuffix(value, "GB")
	case strings.HasSuffix(value, "G"):
		multiplier = 1024 * 1024 * 1024
		value = strings.TrimSuffix(value, "G")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, err
	}
	return n * multiplier, nil
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "unknown"
	}
	if d > 24*time.Hour {
		days := int(d / (24 * time.Hour))
		return fmt.Sprintf("%dd%dh", days, int((d%(24*time.Hour))/time.Hour))
	}
	if d > time.Hour {
		return d.Truncate(time.Minute).String()
	}
	return d.Truncate(time.Second).String()
}

func sameFileSize(path string, size int64) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() == size
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func partExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func prepareTCP(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(false)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}
}

func (s *TransferStats) AddUpload(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.netUpload.Add(n)
}

func (s *TransferStats) AddDownload(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.netDownload.Add(n)
}

func (s *TransferStats) AddDiskRead(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.diskRead.Add(n)
}

func (s *TransferStats) AddDiskWrite(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.diskWrite.Add(n)
}

func (s *TransferStats) Begin(current string) func() {
	if s == nil {
		return func() {}
	}
	s.active.Add(1)
	s.SetCurrent(current)
	return func() {
		s.active.Add(-1)
	}
}

func (s *TransferStats) SetCurrent(current string) {
	if s == nil {
		return
	}
	s.currentMu.Lock()
	s.current = current
	s.currentMu.Unlock()
}

func (s *TransferStats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{}
	}
	s.currentMu.Lock()
	current := s.current
	s.currentMu.Unlock()
	return StatsSnapshot{
		NetUpload:   s.netUpload.Load(),
		NetDownload: s.netDownload.Load(),
		DiskRead:    s.diskRead.Load(),
		DiskWrite:   s.diskWrite.Load(),
		Active:      s.active.Load(),
		Current:     current,
	}
}

func (p *Progress) Add(n int64) {
	p.done.Add(n)
}

func (p *Progress) Done() int64 {
	if p == nil {
		return 0
	}
	return p.done.Load()
}

func (p *Progress) SetCurrent(s string) {
	p.currentMu.Lock()
	p.current = s
	p.currentMu.Unlock()
}

func (p *Progress) Current() string {
	if p == nil {
		return ""
	}
	p.currentMu.Lock()
	defer p.currentMu.Unlock()
	return p.current
}

func (p *Progress) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Print()
		}
	}
}

func (p *Progress) Print() {
	done := p.done.Load()
	now := time.Now()
	elapsed := now.Sub(p.start)
	snap := p.stats.Snapshot()
	deltaSeconds := now.Sub(p.lastAt).Seconds()
	if p.lastAt.IsZero() || deltaSeconds <= 0 {
		deltaSeconds = 1
	}
	uploadSpeed := float64(snap.NetUpload-p.lastUpload) / deltaSeconds
	downloadSpeed := float64(snap.NetDownload-p.lastDownload) / deltaSeconds
	diskReadSpeed := float64(snap.DiskRead-p.lastDiskRead) / deltaSeconds
	diskWriteSpeed := float64(snap.DiskWrite-p.lastDiskWrite) / deltaSeconds
	p.lastAt = now
	p.lastUpload = snap.NetUpload
	p.lastDownload = snap.NetDownload
	p.lastDiskRead = snap.DiskRead
	p.lastDiskWrite = snap.DiskWrite

	averageDoneSpeed := float64(done) / elapsed.Seconds()
	percent := 0.0
	if p.total > 0 {
		percent = float64(done) / float64(p.total) * 100
	}
	eta := "unknown"
	if averageDoneSpeed > 0 && p.total > done {
		eta = formatDuration(time.Duration(float64(p.total-done)/averageDoneSpeed) * time.Second)
	}
	state := "running"
	if p.paused != nil && p.paused.Load() {
		state = "paused"
	}
	p.currentMu.Lock()
	current := p.current
	p.currentMu.Unlock()
	if len(current) > 60 {
		current = "..." + current[len(current)-57:]
	}
	line := fmt.Sprintf("%s / %s  %5.1f%%  down %s/s  up %s/s  write %s/s  read %s/s  ETA %s  %s  %s",
		formatBytes(done),
		formatBytes(p.total),
		percent,
		formatBytes(int64(downloadSpeed)),
		formatBytes(int64(uploadSpeed)),
		formatBytes(int64(diskWriteSpeed)),
		formatBytes(int64(diskReadSpeed)),
		eta,
		state,
		current,
	)
	if len(line) < p.lastLineLen {
		line += strings.Repeat(" ", p.lastLineLen-len(line))
	}
	p.lastLineLen = len(line)
	fmt.Print("\r" + line)
}

func (p *Progress) PrintFinal() {
	p.Print()
	fmt.Println()
}

func (d *ServerDashboard) Run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		d.Print()
	}
}

func (d *ServerDashboard) Print() {
	if d.stats == nil {
		return
	}
	now := time.Now()
	snap := d.stats.Snapshot()
	deltaSeconds := now.Sub(d.lastAt).Seconds()
	if d.lastAt.IsZero() || deltaSeconds <= 0 {
		deltaSeconds = 1
	}
	uploadSpeed := float64(snap.NetUpload-d.lastUpload) / deltaSeconds
	downloadSpeed := float64(snap.NetDownload-d.lastDownload) / deltaSeconds
	diskReadSpeed := float64(snap.DiskRead-d.lastDiskRead) / deltaSeconds
	diskWriteSpeed := float64(snap.DiskWrite-d.lastDiskWrite) / deltaSeconds
	d.lastAt = now
	d.lastUpload = snap.NetUpload
	d.lastDownload = snap.NetDownload
	d.lastDiskRead = snap.DiskRead
	d.lastDiskWrite = snap.DiskWrite

	current := snap.Current
	if len(current) > 70 {
		current = "..." + current[len(current)-67:]
	}
	elapsed := formatDuration(now.Sub(d.start))
	line := fmt.Sprintf("up %s/s (%s)  down %s/s (%s)  read %s/s (%s)  write %s/s (%s)  active %d  elapsed %s  %s",
		formatBytes(int64(uploadSpeed)),
		formatBytes(snap.NetUpload),
		formatBytes(int64(downloadSpeed)),
		formatBytes(snap.NetDownload),
		formatBytes(int64(diskReadSpeed)),
		formatBytes(snap.DiskRead),
		formatBytes(int64(diskWriteSpeed)),
		formatBytes(snap.DiskWrite),
		snap.Active,
		elapsed,
		current,
	)
	if len(line) < d.lastLineLen {
		line += strings.Repeat(" ", d.lastLineLen-len(line))
	}
	d.lastLineLen = len(line)
	fmt.Print("\r" + line)
}
