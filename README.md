# Data Hoarder File Transfer (DHFT)

**Data Hoarder File Transfer (DHFT)** is a direct peer-to-peer file transfer tool
for moving very large folders between two machines as fast as the network and
disks will allow.

It was built for cases where ordinary sync tools spend hours indexing,
processing, or hashing before data starts moving. DHFT starts transferring
immediately, downloads file chunks in parallel, resumes interrupted transfers,
and verifies completed files with MD5.

## Highlights

- Direct TCP peer-to-peer transfer with no relay server.
- Separate GUI apps for server and client on Windows and macOS.
- Command-line mode for scripts and remote shells.
- Parallel chunk downloads for high-latency routes.
- Pause, stop, and resume.
- Crash-safe resume using `.dhft-state.json` and `*.hcpart` partial files.
- MD5 verification after each received file.
- Live upload/download speed plus disk read/write speed.
- Optional shared token so random clients cannot request files.
- Dry-run mode to inspect a transfer before downloading.
- Open source under the GNU GPLv3 license.

## Use Cases

- Moving tens of terabytes between owned servers.
- One-time datacenter, office, NAS, or workstation migrations.
- Long-distance transfers where latency limits single-connection throughput.
- Media archives, research datasets, backups, blockchain snapshots, VM images,
  and other large file collections.
- Transfers where you want explicit pause/resume and final integrity checks
  without running a full bidirectional sync system.

## How It Works

Run the **server** app on the machine that already has the files. Run the
**client** app on the machine that should receive the files.

The server listens on a TCP port. The client connects directly to that server,
requests a folder manifest, then downloads missing chunks in parallel. When a
file is complete, the client asks the server for the file MD5 and compares it
with the received copy.

DHFT does not need a cloud service, database, account, or relay server.

## Security Notes

The shared token is authentication, not encryption. If the transfer crosses the
public internet and the data is sensitive, run DHFT over a VPN or encrypted
tunnel such as WireGuard, Tailscale, ZeroTier, or SSH tunneling.

Only expose the server port to the client IPs you trust.

## Install

Download the latest release package for each machine:

- `DHFT-Server-Windows.zip`
- `DHFT-Client-Windows.zip`
- `DHFT-Server-macOS.zip`
- `DHFT-Client-macOS.zip`

Use the **server** package on the uploader machine and the **client** package on
the downloader machine.

### Windows

1. Extract the ZIP.
2. Double-click `DHFT-Server-Windows.exe` or `DHFT-Client-Windows.exe`.
3. If Windows Firewall asks, allow the server app to accept connections.

### macOS

1. Extract the ZIP.
2. Open `DHFT Server.app` or `DHFT Client.app`.
3. If macOS blocks the unsigned app, right-click it, choose **Open**, then
   confirm.

The apps open a local browser GUI. The browser is only the control panel; the
file transfer remains direct between the two machines.

## GUI Usage

### Server

1. Choose the folder to upload.
2. Set the listen address, usually `:9811`.
3. Set a long shared token.
4. Press **Start**.
5. Make sure the client can reach the server IP and port.

The server dashboard shows upload speed, download/control-message speed, disk
read speed, disk write speed, active chunk requests, elapsed time, and the
current file activity.

### Client

1. Enter the server address, for example `203.0.113.10:9811`.
2. Choose the destination folder.
3. Enter the same shared token.
4. Choose worker count and chunk size.
5. Press **Start**.

The client dashboard shows total progress, download speed, upload/control speed,
disk write speed, disk read speed, active requests, file count, pause state, and
current file activity.

## Recommended Settings

Start with:

- Workers: `8`
- Chunk size: `64M`

For fast long-distance links, try:

- Workers: `12` or `16`
- Chunk size: `128M`

If the disk is the bottleneck, reduce workers. If the network is underused,
increase workers before increasing chunk size.

## Command-Line Usage

The same binary also supports command-line mode.

Run on the uploader:

```bash
dhft serve --dir /path/to/files --listen :9811 --token "choose-a-long-secret"
```

Run on the downloader:

```bash
dhft receive \
  --addr SERVER_IP_OR_DNS:9811 \
  --dest /path/to/destination \
  --token "choose-a-long-secret" \
  --workers 12 \
  --chunk-size 128M
```

Pause or resume command-line transfers by typing `p` then Enter. Stop with `q`
then Enter. Rerun the same receive command to resume.

Useful flags:

```bash
dhft receive --dry-run
dhft receive --skip-existing
dhft receive --verify none
dhft receive --dashboard=false
```

## Build From Source

Requirements:

- Go 1.25 or newer.
- macOS with `lipo` if you want universal macOS release packages.

Build a local binary:

```bash
go build -o dhft .
```

Run tests:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Build release packages:

```bash
./scripts/build-packages.sh
```

Release ZIPs are written to `packages/`.

## Repository Layout

- `main.go`: transfer protocol, server/client CLI, resume, hashing, stats.
- `gui.go`: local browser GUI server and API.
- `main_test.go`: protocol, transfer, resume, GUI API, and safety tests.
- `packaging/`: package README files and macOS app metadata.
- `scripts/`: build and packaging helpers.

## License

DHFT is licensed under the **GNU General Public License v3.0**. See
[LICENSE](LICENSE).
