#!/bin/bash
# hopsfs-churn-dn.sh
#
# Periodic DataNode kill/start loop driven over the HopsFS standalone
# control socket. An external driver that you can start, pause, and
# stop independently of the cluster. The cluster must be running with
# --ctl-port=<port> (default 7777).

set -euo pipefail

INTERVAL="${1:-60}"
DN_IDX="${2:-0}"
PORT="${HOPSFS_CTL_PORT:-7777}"
HOST="${HOPSFS_CTL_HOST:-localhost}"

cat <<EOF
================================================================================
HopsFS DataNode churn — periodic kill+start of DN ${DN_IDX}
================================================================================

Target:        ${HOST}:${PORT}
DN index:      ${DN_IDX}
Interval:      ${INTERVAL} s   (down ${INTERVAL} s, then up ${INTERVAL} s, repeat)
Full cycle:    $((INTERVAL * 2)) s

Loop body:
  1. send 'kill dn ${DN_IDX}' over the control socket
        - If async cloud upload is on, the runner drains the DN first
          (DataNode.drainAndSuspend, 600 s cap) and prints the drain
          status line before the stop completes.
        - If async is off, the DN is stopped immediately.
  2. sleep ${INTERVAL} s   (DN is dead — NN should expire it in ~20 s)
  3. send 'start dn ${DN_IDX}' over the control socket
        - A fresh DataNode instance comes up with drainPhase=NORMAL
          and re-registers with the NN; writes resume after the next
          heartbeat round.
  4. sleep ${INTERVAL} s   (DN is live)
  -> repeat

Prerequisites:
  - HopsFS standalone runner is up with --ctl-port=${PORT}.
  - At least 2 DataNodes so the cluster stays usable while DN ${DN_IDX} is down
    (otherwise writes will fail during the down phase).
  - 'nc' is on PATH.

Press Ctrl+C to stop. A stop mid-cycle leaves the DN in whatever state
the last command put it in — re-run 'start dn ${DN_IDX}' via hopsfs-ctl.sh
to bring it back if needed.

Usage:
  $(basename "$0") [interval-secs] [dn-idx]
    interval-secs   Default: 60. Down-time and up-time each use this value.
    dn-idx          Default: 0. Which DN to churn.

Environment overrides:
  HOPSFS_CTL_HOST   Default: ${HOST}
  HOPSFS_CTL_PORT   Default: ${PORT}

================================================================================
EOF

if ! command -v nc >/dev/null 2>&1; then
  echo "ERROR: 'nc' (netcat) is not on PATH. Install it and re-run." >&2
  exit 1
fi

# Bound the connection so a slow drain doesn't outpace the next cycle.
# 'kill dn' may take up to ~600 s when async drain runs; use a slightly
# higher cap to keep the loop predictable.
NC_KILL_TIMEOUT=$((INTERVAL > 620 ? INTERVAL : 620))
NC_START_TIMEOUT=10

send_cmd() {
  local cmd="$1"
  local wait="$2"
  echo "[$(date +%T)] ${cmd}"
  printf '%s\n' "${cmd}" | nc -w "${wait}" "${HOST}" "${PORT}" | sed 's/^/    /'
}

trap 'echo; echo "[churn-dn] stopping."; exit 0' INT TERM

while true; do
  send_cmd "kill dn ${DN_IDX}" "${NC_KILL_TIMEOUT}"
  sleep "${INTERVAL}"
  send_cmd "start dn ${DN_IDX}" "${NC_START_TIMEOUT}"
  sleep "${INTERVAL}"
done
