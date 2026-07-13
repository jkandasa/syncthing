#!/usr/bin/env bash
# verify-s3-backend.sh — End-to-end verification of the Syncthing S3 backend.
#
# This script verifies the full S3 filesystem backend by performing real
# filesystem operations against a local MinIO instance, checking that:
#   - Files can be created, read, and deleted
#   - Directories can be created and listed
#   - POSIX metadata (permissions, ownership, timestamps) is preserved
#   - File renaming works correctly
#   - Nested directory trees work
#   - The S3 filesystem type is properly registered
#
# Prerequisites:
#   - MinIO server on PATH
#   - Go toolchain available
#
# Usage:
#   ./script/verify-s3-backend.sh

set -euo pipefail

MINIO_PORT=${MINIO_PORT:-9097}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
MINIO_DATA_DIR=$(mktemp -d)
ENDPOINT="localhost:${MINIO_PORT}"

PASS=0
FAIL=0

pass() {
    echo "  ✓ $1"
    PASS=$((PASS + 1))
}

fail() {
    echo "  ✗ $1"
    FAIL=$((FAIL + 1))
}

cleanup() {
    if [ -n "${MINIO_PID:-}" ] && kill -0 "$MINIO_PID" 2>/dev/null; then
        kill "$MINIO_PID" 2>/dev/null || true
        wait "$MINIO_PID" 2>/dev/null || true
    fi
    rm -rf "$MINIO_DATA_DIR"
}
trap cleanup EXIT

echo "=================================================="
echo "  Syncthing S3 Backend — Verification Script"
echo "=================================================="
echo ""

# ── Start MinIO ──
echo "Starting MinIO on port ${MINIO_PORT}..."
MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
    minio server "$MINIO_DATA_DIR" \
    --address ":${MINIO_PORT}" \
    --console-address ":$((MINIO_PORT + 1))" \
    --quiet &
MINIO_PID=$!

for i in $(seq 1 30); do
    if curl -s "http://${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: MinIO failed to start"
        exit 1
    fi
    sleep 1
done
echo "MinIO is ready."
echo ""

# ── Run unit tests ──
echo "Test Suite 1: Unit Tests"
echo "------------------------"
if S3_TEST_ENDPOINT="$ENDPOINT" \
    S3_TEST_ACCESS_KEY="$MINIO_ROOT_USER" \
    S3_TEST_SECRET_KEY="$MINIO_ROOT_PASSWORD" \
    go test ./lib/fs/s3fs/ -count=1 -timeout 120s >/dev/null 2>&1; then
    pass "All unit tests pass"
else
    fail "Unit tests failed"
fi
echo ""

# ── Run a Go integration test program ──
echo "Test Suite 2: Integration Verification"
echo "---------------------------------------"

VERIFY_PROG=$(mktemp /tmp/s3verify_XXXXXX.go)
cat > "$VERIFY_PROG" << 'GOEOF'
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/syncthing/syncthing/lib/fs/s3fs"
	"github.com/syncthing/syncthing/lib/fs"
)

func main() {
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
	secretKey := os.Getenv("S3_TEST_SECRET_KEY")
	bucket := "syncthing-verify-" + fmt.Sprintf("%d", time.Now().UnixNano())

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		fmt.Printf("FAIL: create client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		fmt.Printf("FAIL: create bucket: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		_ = client.RemoveBucket(ctx, bucket)
	}()

	sfs := s3fs.NewS3FilesystemFromClient(client, bucket, "")
	pass := 0
	fail := 0

	check := func(name string, ok bool) {
		if ok {
			fmt.Printf("  PASS: %s\n", name)
			pass++
		} else {
			fmt.Printf("  FAIL: %s\n", name)
			fail++
		}
	}

	// Test 1: Filesystem type
	check("Type is s3", sfs.Type() == s3fs.FilesystemTypeS3)

	// Test 2: Create and read file
	f, _ := sfs.Create("verify.txt")
	f.Write([]byte("verification content"))
	f.Close()
	f2, _ := sfs.Open("verify.txt")
	data, _ := io.ReadAll(f2)
	f2.Close()
	check("Create and read file", string(data) == "verification content")

	// Test 3: Stat file
	fi, err := sfs.Stat("verify.txt")
	check("Stat file", err == nil && fi.Size() == 20 && !fi.IsDir())

	// Test 4: Mkdir and stat
	sfs.Mkdir("testdir", 0o755)
	fi, err = sfs.Stat("testdir")
	check("Mkdir and stat directory", err == nil && fi.IsDir())

	// Test 5: DirNames
	fa, _ := sfs.Create("testdir/a.txt"); fa.Close()
	fb, _ := sfs.Create("testdir/b.txt"); fb.Close()
	names, _ := sfs.DirNames("testdir")
	sort.Strings(names)
	check("DirNames", len(names) == 2 && names[0] == "a.txt" && names[1] == "b.txt")

	// Test 6: Permissions via Chmod
	sfs.Chmod("verify.txt", 0o755)
	fi, _ = sfs.Stat("verify.txt")
	check("Chmod preserves permissions", fi.Mode()&fs.ModePerm == 0o755)

	// Test 7: Ownership via Lchown
	sfs.Lchown("verify.txt", "1000", "2000")
	fi, _ = sfs.Stat("verify.txt")
	check("Lchown preserves ownership", fi.Owner() == 1000 && fi.Group() == 2000)

	// Test 8: Timestamps via Chtimes
	mtime := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)
	atime := time.Date(2023, 1, 15, 11, 30, 0, 0, time.UTC)
	sfs.Chtimes("verify.txt", atime, mtime)
	fi, _ = sfs.Stat("verify.txt")
	diff := fi.ModTime().Sub(mtime)
	check("Chtimes preserves mtime", diff > -time.Second && diff < time.Second)

	// Test 9: Rename
	sfs.Rename("verify.txt", "renamed.txt")
	_, errOld := sfs.Stat("verify.txt")
	_, errNew := sfs.Stat("renamed.txt")
	check("Rename works", fs.IsNotExist(errOld) && errNew == nil)

	// Test 10: Remove
	sfs.Remove("renamed.txt")
	_, err = sfs.Stat("renamed.txt")
	check("Remove works", fs.IsNotExist(err))

	// Test 11: MkdirAll
	sfs.MkdirAll("deep/nested/dir", 0o755)
	fi, err = sfs.Stat("deep/nested/dir")
	check("MkdirAll creates nested directories", err == nil && fi.IsDir())

	// Test 12: RemoveAll
	sfs.RemoveAll("deep")
	_, err = sfs.Stat("deep")
	check("RemoveAll removes tree", fs.IsNotExist(err))

	// Test 13: Root stat
	fi, err = sfs.Stat(".")
	check("Stat root directory", err == nil && fi.IsDir())

	// Test 14: Symlink
	sfs.CreateSymlink("target", "symlink")
	target, _ := sfs.ReadSymlink("symlink")
	check("Symlink create and read", target == "target")

	// Test 15: WriteAt / ReadAt
	wf, _ := sfs.OpenFile("writeat.bin", os.O_RDWR|os.O_CREATE, 0o666)
	wf.Write([]byte("AAAAAAAAAA"))
	wf.WriteAt([]byte("BB"), 3)
	wf.Close()
	rf, _ := sfs.Open("writeat.bin")
	rdata, _ := io.ReadAll(rf)
	rf.Close()
	check("WriteAt/ReadAt", bytes.Equal(rdata, []byte("AAABBAAAAA")))

	// Test 16: Glob
	g1, _ := sfs.Create("glob1.txt"); g1.Close()
	g2, _ := sfs.Create("glob2.txt"); g2.Close()
	g3, _ := sfs.Create("glob3.log"); g3.Close()
	matches, _ := sfs.Glob("glob*.txt")
	sort.Strings(matches)
	check("Glob pattern matching", len(matches) == 2)

	// Test 17: Truncate
	tf, _ := sfs.OpenFile("trunc.txt", os.O_RDWR|os.O_CREATE, 0o666)
	tf.Write([]byte("Hello World"))
	tf.Truncate(5)
	tf.Close()
	tf2, _ := sfs.Open("trunc.txt")
	tdata, _ := io.ReadAll(tf2)
	tf2.Close()
	check("Truncate", string(tdata) == "Hello")

	// Test 18: Usage (should return large values for S3)
	usage, _ := sfs.Usage(".")
	check("Usage returns non-zero", usage.Free > 0 && usage.Total > 0)

	fmt.Printf("\nResults: %d passed, %d failed\n", pass, fail)
	if fail > 0 {
		os.Exit(1)
	}
}
GOEOF

# Copy the Go program into a temp module directory
VERIFY_DIR=$(mktemp -d)
mkdir -p "$VERIFY_DIR"
cp "$VERIFY_PROG" "$VERIFY_DIR/main.go"
rm -f "$VERIFY_PROG"

# Create a go.mod that requires the syncthing module
cd "$VERIFY_DIR"
cat > go.mod << EOF
module s3verify

go 1.25.0

require (
	github.com/minio/minio-go/v7 v7.2.1
	github.com/syncthing/syncthing v0.0.0
)

replace github.com/syncthing/syncthing => /testbed/syncthing
EOF

go mod tidy 2>/dev/null
S3_TEST_ENDPOINT="$ENDPOINT" \
    S3_TEST_ACCESS_KEY="$MINIO_ROOT_USER" \
    S3_TEST_SECRET_KEY="$MINIO_ROOT_PASSWORD" \
    go run main.go

INTEG_EXIT=$?
cd /testbed/syncthing
rm -rf "$VERIFY_DIR"

if [ $INTEG_EXIT -eq 0 ]; then
    pass "Integration verification program passed"
else
    fail "Integration verification program failed"
fi

echo ""
echo "=================================================="
echo "  Verification Summary"
echo "=================================================="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo ""

if [ $FAIL -gt 0 ]; then
    echo "  ✗ VERIFICATION FAILED"
    exit 1
else
    echo "  ✓ ALL VERIFICATIONS PASSED"
    exit 0
fi
