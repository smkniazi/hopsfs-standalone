#!/bin/bash
# hopsfs-churn-nn.sh
#
# Periodic NameNode kill/start loop driven over the HopsFS standalone
# control socket. An external driver that you can start, pause, and
# stop independently of the cluster. The cluster must be running with
# --ctl-port=<port> (default 7777).

set -euo pipefail

INTERVAL="${1:-60}"
NN_IDX="${2:-0}"
PORT="${HOPSFS_CTL_PORT:-7777}"
HOST="${HOPSFS_CTL_HOST:-localhost}"

cat <<EOF
================================================================================
HopsFS NameNode churn — periodic kill+start of NN ${NN_IDX}
================================================================================

Target:        ${HOST}:${PORT}
NN index:      ${NN_IDX}
Interval:      ${INTERVAL} s   (down ${INTERVAL} s, then up ${INTERVAL} s, repeat)
Full cycle:    $((INTERVAL * 2)) s

Loop body:
  1. send 'kill nn ${NN_IDX}' over the control socket
  2. sleep ${INTERVAL} s   (NN is down)
  3. send 'start nn ${NN_IDX}' over the control socket
  4. sleep ${INTERVAL} s   (NN is live)
  -> repeat

Prerequisites:
  - HopsFS standalone runner is up with --ctl-port=${PORT}.
  - At least 2 NameNodes — clients of the killed NN should fail over to
    a sibling while ${NN_IDX} is down. With a single NN the cluster is
    unreachable during the down phase.
  - 'nc' is on PATH.

Note: if you churn NN 0 (the one whose IPC port is pinned via
--namenode-port), clients that connect using the written-out hdfs-site.xml
will see the configured port disappear while it's killed. Standby NNs at
higher indices use ephemeral ports; killing those is less disruptive.

Press Ctrl+C to stop. A stop mid-cycle leaves the NN in whatever state
the last command put it in — re-run 'start nn ${NN_IDX}' via hopsfs-ctl.sh
to bring it back if needed.

Usage:
  $(basename "$0") [interval-secs] [nn-idx]
    interval-secs   Default: 60. Down-time and up-time each use this value.
    nn-idx          Default: 0. Which NN to churn.

Environment overrides:
  HOPSFS_CTL_HOST   Default: ${HOST}
  HOPSFS_CTL_PORT   Default: ${PORT}

================================================================================
EOF

if ! command -v nc >/dev/null 2>&1; then
  echo "ERROR: 'nc' (netcat) is not on PATH. Install it and re-run." >&2
  exit 1
fi

send_cmd() {
  local cmd="$1"
  echo "[$(date +%T)] ${cmd}"
  # -w bounds the total connection time so a wedged server doesn't hang the loop.
  printf '%s\n' "${cmd}" | nc -w 10 "${HOST}" "${PORT}" | sed 's/^/    /'
}

trap 'echo; echo "[churn-nn] stopping."; exit 0' INT TERM

while true; do
  send_cmd "kill nn ${NN_IDX}"
  sleep "${INTERVAL}"
  send_cmd "start nn ${NN_IDX}"
  sleep "${INTERVAL}"
done
