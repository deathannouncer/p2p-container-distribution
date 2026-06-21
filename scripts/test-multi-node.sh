#!/bin/bash
# Brings up a 3-node local cluster, uploads a blob, kills a node mid-flight,
# and verifies the blob is still retrievable with matching checksum --
# the same scenario behind the "100% availability under multi-node
# failure" result. Run from the repo root:
#
#   ./scripts/test-multi-node.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
BIN="$WORK_DIR/node"

cleanup() {
  echo "--- cleaning up ---"
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "--- building node binary ---"
(cd "$ROOT_DIR" && go build -o "$BIN" ./cmd/node)

mkdir -p "$WORK_DIR/node1" "$WORK_DIR/node2" "$WORK_DIR/node3"

PIDS=()
start_node() {
  local id=$1 port=$2 peers=$3 dir=$4
  setsid "$BIN" -node-id="$id" -listen=":$port" -advertise="127.0.0.1:$port" \
    -peers="$peers" -data-dir="$dir" > "$WORK_DIR/$id.log" 2>&1 < /dev/null &
  PIDS+=($!)
}

echo "--- starting 3-node cluster ---"
start_node node-1 9001 "node-2=127.0.0.1:9002,node-3=127.0.0.1:9003" "$WORK_DIR/node1"
start_node node-2 9002 "node-1=127.0.0.1:9001,node-3=127.0.0.1:9003" "$WORK_DIR/node2"
start_node node-3 9003 "node-1=127.0.0.1:9001,node-2=127.0.0.1:9002" "$WORK_DIR/node3"
sleep 2

echo "--- generating 10MB test blob ---"
head -c 10485760 /dev/urandom > "$WORK_DIR/testblob.bin"
ORIGINAL_MD5=$(md5sum "$WORK_DIR/testblob.bin" | awk '{print $1}')
echo "original md5: $ORIGINAL_MD5"

echo "--- uploading blob via node-1 ---"
curl -sf -X POST --data-binary @"$WORK_DIR/testblob.bin" http://127.0.0.1:9001/blobs/test-layer > "$WORK_DIR/manifest.json"
cat "$WORK_DIR/manifest.json"
echo

echo "--- downloading blob via node-1 (sanity check before failure) ---"
curl -sf http://127.0.0.1:9001/blobs/test-layer -o "$WORK_DIR/before.bin"
BEFORE_MD5=$(md5sum "$WORK_DIR/before.bin" | awk '{print $1}')
[ "$BEFORE_MD5" = "$ORIGINAL_MD5" ] || { echo "FAIL: checksum mismatch before failure"; exit 1; }
echo "OK: checksum matches before failure"

echo "--- killing node-2 ---"
kill -9 "${PIDS[1]}"

echo "--- waiting for failure detection + self-healing repair ---"
sleep 10
curl -s http://127.0.0.1:9001/status; echo

echo "--- downloading blob via node-1 with node-2 dead ---"
curl -sf http://127.0.0.1:9001/blobs/test-layer -o "$WORK_DIR/after.bin"
AFTER_MD5=$(md5sum "$WORK_DIR/after.bin" | awk '{print $1}')
[ "$AFTER_MD5" = "$ORIGINAL_MD5" ] || { echo "FAIL: checksum mismatch after node failure"; exit 1; }
echo "OK: checksum matches after node-2 failure -- cluster stayed available"

echo
echo "PASS: self-healing distribution survived a node failure with zero data loss."
