#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/debug/p2p-dfs-node}"
WORK_DIR="${WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/p2p-dfs-demo.XXXXXX")}"

P2P_PORT_A="${P2P_PORT_A:-4101}"
P2P_PORT_B="${P2P_PORT_B:-4102}"
GRPC_PORT_A="${GRPC_PORT_A:-51051}"
GRPC_PORT_B="${GRPC_PORT_B:-51052}"

GRPC_ADDR_A="127.0.0.1:${GRPC_PORT_A}"
GRPC_ADDR_B="127.0.0.1:${GRPC_PORT_B}"

NODE_A_DIR="$WORK_DIR/node-a"
NODE_B_DIR="$WORK_DIR/node-b"
LOG_DIR="$WORK_DIR/logs"
DOWNLOAD_DIR="$WORK_DIR/downloads"
SOURCE_FILE="$WORK_DIR/demo-file.txt"
OUTPUT_FILE="$DOWNLOAD_DIR/demo-file.txt"

PID_A=""
PID_B=""

cleanup() {
    local exit_code="$1"

    set +e
    if [[ -n "$PID_A" ]] && kill -0 "$PID_A" 2>/dev/null; then
        kill "$PID_A" 2>/dev/null || true
        wait "$PID_A" 2>/dev/null || true
    fi

    if [[ -n "$PID_B" ]] && kill -0 "$PID_B" 2>/dev/null; then
        kill "$PID_B" 2>/dev/null || true
        wait "$PID_B" 2>/dev/null || true
    fi

    if [[ "${KEEP_WORKDIR:-0}" == "1" ]]; then
        echo
        echo "Demo artefacts kept in: $WORK_DIR"
    else
        rm -rf "$WORK_DIR"
    fi

    exit "$exit_code"
}

trap 'cleanup $?' EXIT

build_binary() {
    if [[ -x "$BIN" ]]; then
        return
    fi

    echo "Building p2p-dfs-node..."
    (cd "$ROOT_DIR" && cargo build --bin p2p-dfs-node)
}

wait_for_daemon() {
    local grpc_addr="$1"

    for _ in $(seq 1 60); do
        if "$BIN" status --grpc-addr "http://${grpc_addr}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done

    echo "Timed out waiting for daemon at ${grpc_addr}" >&2
    return 1
}

peer_id_for() {
    local grpc_addr="$1"

    "$BIN" status --grpc-addr "http://${grpc_addr}" | awk -F': ' '/^peer_id: / {print $2}'
}

wait_for_download() {
    local grpc_addr="$1"
    local cid="$2"

    for _ in $(seq 1 80); do
        local status_output
        status_output="$("$BIN" download-status "$cid" --grpc-addr "http://${grpc_addr}" 2>/dev/null || true)"

        if grep -q "^phase: Completed$" <<<"$status_output"; then
            return 0
        fi

        if grep -Eq "^phase: (Failed|Cancelled)$" <<<"$status_output"; then
            echo "$status_output" >&2
            return 1
        fi

        sleep 0.5
    done

    echo "Timed out waiting for download completion for ${cid}" >&2
    return 1
}

print_step() {
    printf '\n==> %s\n' "$1"
}

mkdir -p "$NODE_A_DIR" "$NODE_B_DIR" "$LOG_DIR" "$DOWNLOAD_DIR"
build_binary

cat >"$SOURCE_FILE" <<'EOF'
This file was exchanged in a tiny two-node demo.
Node A adds it, provides it through Kademlia, and Node B downloads it.
EOF

print_step "Starting node A"
"$BIN" daemon \
    --listen-p2p "/ip4/127.0.0.1/tcp/${P2P_PORT_A}" \
    --grpc-addr "${GRPC_ADDR_A}" \
    --db-path "${NODE_A_DIR}/db" \
    --key-file "${NODE_A_DIR}/node_key.ed25519" \
    --mdns false \
    >"${LOG_DIR}/node-a.log" 2>&1 &
PID_A="$!"
wait_for_daemon "$GRPC_ADDR_A"

NODE_A_PEER_ID="$(peer_id_for "$GRPC_ADDR_A")"
NODE_A_ADDR="/ip4/127.0.0.1/tcp/${P2P_PORT_A}/p2p/${NODE_A_PEER_ID}"

print_step "Starting node B"
"$BIN" daemon \
    --listen-p2p "/ip4/127.0.0.1/tcp/${P2P_PORT_B}" \
    --grpc-addr "${GRPC_ADDR_B}" \
    --peer "${NODE_A_ADDR}" \
    --db-path "${NODE_B_DIR}/db" \
    --key-file "${NODE_B_DIR}/node_key.ed25519" \
    --mdns false \
    >"${LOG_DIR}/node-b.log" 2>&1 &
PID_B="$!"
wait_for_daemon "$GRPC_ADDR_B"

print_step "Node status"
"$BIN" status --grpc-addr "http://${GRPC_ADDR_A}"
"$BIN" status --grpc-addr "http://${GRPC_ADDR_B}"

print_step "Adding and providing content from node A"
CID="$("$BIN" add "$SOURCE_FILE" --grpc-addr "http://${GRPC_ADDR_A}")"
"$BIN" provide "$CID" --grpc-addr "http://${GRPC_ADDR_A}" >/dev/null
echo "CID: $CID"
echo "Providing set on node A:"
"$BIN" providing --grpc-addr "http://${GRPC_ADDR_A}"

print_step "Downloading content from node B"
"$BIN" get "$CID" -o "$OUTPUT_FILE" --grpc-addr "http://${GRPC_ADDR_B}" >/dev/null
wait_for_download "$GRPC_ADDR_B" "$CID"

if ! cmp -s "$SOURCE_FILE" "$OUTPUT_FILE"; then
    echo "Downloaded file does not match source file" >&2
    exit 1
fi

print_step "Download completed"
echo "Downloaded file matches the source bytes."
echo "Output file: $OUTPUT_FILE"
echo
echo "Node B download status:"
"$BIN" download-status "$CID" --grpc-addr "http://${GRPC_ADDR_B}"
