#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

rm -rf build package_tmp packages
mkdir -p build packages

echo "Building Windows GUI apps..."
GOOS=windows GOARCH=amd64 go build -ldflags="-H=windowsgui" -o build/DHFT-Server-Windows.exe .
GOOS=windows GOARCH=amd64 go build -ldflags="-H=windowsgui" -o build/DHFT-Client-Windows.exe .

echo "Building macOS GUI apps..."
GOOS=darwin GOARCH=arm64 go build -o build/DHFT-Server-macOS-arm64 .
GOOS=darwin GOARCH=arm64 go build -o build/DHFT-Client-macOS-arm64 .
GOOS=darwin GOARCH=amd64 go build -o build/DHFT-Server-macOS-amd64 .
GOOS=darwin GOARCH=amd64 go build -o build/DHFT-Client-macOS-amd64 .

if command -v lipo >/dev/null 2>&1; then
  lipo -create -output build/DHFT-Server-macOS build/DHFT-Server-macOS-arm64 build/DHFT-Server-macOS-amd64
  lipo -create -output build/DHFT-Client-macOS build/DHFT-Client-macOS-arm64 build/DHFT-Client-macOS-amd64
else
  echo "lipo is not available; using arm64 macOS binaries only."
  cp build/DHFT-Server-macOS-arm64 build/DHFT-Server-macOS
  cp build/DHFT-Client-macOS-arm64 build/DHFT-Client-macOS
fi
chmod +x build/DHFT-Server-macOS build/DHFT-Client-macOS

echo "Assembling packages..."
mkdir -p package_tmp/DHFT-Server-Windows package_tmp/DHFT-Client-Windows
mkdir -p "package_tmp/DHFT-Server-macOS/DHFT Server.app/Contents/MacOS" "package_tmp/DHFT-Server-macOS/DHFT Server.app/Contents/Resources"
mkdir -p "package_tmp/DHFT-Client-macOS/DHFT Client.app/Contents/MacOS" "package_tmp/DHFT-Client-macOS/DHFT Client.app/Contents/Resources"

cp build/DHFT-Server-Windows.exe package_tmp/DHFT-Server-Windows/
cp packaging/server-windows-readme.txt package_tmp/DHFT-Server-Windows/README.txt

cp build/DHFT-Client-Windows.exe package_tmp/DHFT-Client-Windows/
cp packaging/client-windows-readme.txt package_tmp/DHFT-Client-Windows/README.txt

cp build/DHFT-Server-macOS "package_tmp/DHFT-Server-macOS/DHFT Server.app/Contents/MacOS/DHFT-Server-macOS"
cp packaging/server-macos-info.plist "package_tmp/DHFT-Server-macOS/DHFT Server.app/Contents/Info.plist"
cp packaging/server-macos-readme.txt package_tmp/DHFT-Server-macOS/README.txt

cp build/DHFT-Client-macOS "package_tmp/DHFT-Client-macOS/DHFT Client.app/Contents/MacOS/DHFT-Client-macOS"
cp packaging/client-macos-info.plist "package_tmp/DHFT-Client-macOS/DHFT Client.app/Contents/Info.plist"
cp packaging/client-macos-readme.txt package_tmp/DHFT-Client-macOS/README.txt

chmod +x "package_tmp/DHFT-Server-macOS/DHFT Server.app/Contents/MacOS/DHFT-Server-macOS"
chmod +x "package_tmp/DHFT-Client-macOS/DHFT Client.app/Contents/MacOS/DHFT-Client-macOS"

(cd package_tmp && zip -qr ../packages/DHFT-Server-Windows.zip DHFT-Server-Windows)
(cd package_tmp && zip -qr ../packages/DHFT-Client-Windows.zip DHFT-Client-Windows)
(cd package_tmp && zip -qr ../packages/DHFT-Server-macOS.zip DHFT-Server-macOS)
(cd package_tmp && zip -qr ../packages/DHFT-Client-macOS.zip DHFT-Client-macOS)

echo "Packages written to $ROOT_DIR/packages:"
ls -lh packages
