#!/usr/bin/env bash
# test-s3-manual.sh — Manual testing script for the Syncthing S3 backend.
#
# Prerequisites:
#   - MinIO installed and on PATH (or docker available)
#   - Go toolchain installed
#
# This script:
#   1. Starts a local MinIO instance
#   2. Creates a test bucket
#   3. Runs the S3 filesystem unit test suite
#   4. Demonstrates filesystem operations interactively
#   5. Cleans up
#
# Usage:
#   ./script/test-s3-manual.sh

set -euo pipefail

MINIO_PORT=${MINIO_PORT:-9099}
MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-9098}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
MINIO_DATA_DIR=$(mktemp -d)
TEST_BUCKET="syncthing-manual-test"
ENDPOINT="localhost:${MINIO_PORT}"

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    if [ -n "${MINIO_PID:-}" ] && kill -0 "$MINIO_PID" 2>/dev/null; then
        kill "$MINIO_PID" 2>/dev/null || true
        wait "$MINIO_PID" 2>/dev/null || true
        echo "MinIO stopped (PID $MINIO_PID)"
    fi
    rm -rf "$MINIO_DATA_DIR"
    echo "Temporary data directory removed"
}
trap cleanup EXIT

echo "============================================"
echo "  Syncthing S3 Backend — Manual Test Script"
echo "============================================"
echo ""

# ── Step 1: Start MinIO ──
echo "=== Step 1: Starting MinIO ==="
echo "  Endpoint:  http://${ENDPOINT}"
echo "  User:      ${MINIO_ROOT_USER}"
echo "  Password:  ${MINIO_ROOT_PASSWORD}"
echo "  Data dir:  ${MINIO_DATA_DIR}"
echo ""

MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
    minio server "$MINIO_DATA_DIR" \
    --address ":${MINIO_PORT}" \
    --console-address ":${MINIO_CONSOLE_PORT}" \
    --quiet &
MINIO_PID=$!

# Wait for MinIO to be ready
echo "Waiting for MinIO to become ready..."
for i in $(seq 1 30); do
    if curl -s "http://${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo "MinIO is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: MinIO failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done
echo ""

# ── Step 2: Run unit tests ──
echo "=== Step 2: Running S3 filesystem unit tests ==="
echo ""
S3_TEST_ENDPOINT="$ENDPOINT" \
    S3_TEST_ACCESS_KEY="$MINIO_ROOT_USER" \
    S3_TEST_SECRET_KEY="$MINIO_ROOT_PASSWORD" \
    go test -v ./lib/fs/s3fs/ -count=1 -timeout 120s

echo ""
echo "=== All unit tests passed! ==="
echo ""

# ── Step 3: Interactive demo ──
echo "=== Step 3: Filesystem operations demo ==="
echo ""
echo "The S3 filesystem can be created with a URI like:"
echo "  s3://${ENDPOINT}/${TEST_BUCKET}?accessKey=${MINIO_ROOT_USER}&secretKey=${MINIO_ROOT_PASSWORD}"
echo ""
echo "In a Syncthing config.xml, set:"
echo "  <folder id=\"my-s3-folder\" filesystemType=\"s3\" "
echo "    path=\"s3://${ENDPOINT}/${TEST_BUCKET}?accessKey=${MINIO_ROOT_USER}&secretKey=${MINIO_ROOT_PASSWORD}\">"
echo ""
echo "See docs/s3-backend.md for full documentation."
echo ""

echo "============================================"
echo "  All tests passed successfully!"
echo "============================================"
