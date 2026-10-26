#!/bin/bash
# hopsfs-ctl.sh
#
# Interactive front-end for the HopsFS standalone control socket. Prints
# usage, then exec's `nc` so you can type commands directly. The cluster
# must already be running with --ctl-port=<port> (default 7777).
#
# All work happens inside the running JVM — this script just shuffles
# bytes between your terminal and the socket.

set -euo pipefail

PORT="${HOPSFS_CTL_PORT:-7777}"
HOST="${HOPSFS_CTL_HOST:-localhost}"

cat <<EOF
================================================================================
HopsFS control socket — interactive session
================================================================================

Target: ${HOST}:${PORT}

The cluster's standalone runner exposes a plain-text TCP socket bound to
loopback. This script just wraps \`nc\` and shows the supported commands
before connecting. Type one command per line. Responses come back on the
next line. Type 'quit' or hit Ctrl+D to disconnect; the cluster keeps
running.

Commands:
  help                 Show the cluster's own help text
  list                 Show NN/DN status (running vs stopped)
  kill dn <idx>        Stop DataNode at index <idx> (data dir preserved)
  kill nn <idx>        Stop NameNode at index <idx>
  start dn <idx>       Start a previously stopped DataNode
  start nn <idx>       Start a previously stopped NameNode
  quit                 Disconnect this session (cluster keeps running)

Examples:
  > list
  > kill dn 0
  > list                # confirm DN 0 is in the "stopped" list
  > start dn 0
  > quit

Environment overrides:
  HOPSFS_CTL_HOST       Host to connect to       (default: ${HOST})
  HOPSFS_CTL_PORT       Port to connect to       (default: ${PORT})

Cluster must be started with the control socket enabled, e.g.:
  java -jar target/hopsfs-standalone-1.0-SNAPSHOT.jar \\
       --num-namenodes=2 --num-datanodes=2 --ctl-port=${PORT}

If 'nc' is not installed, your distribution's netcat package
(typically 'netcat-openbsd' on Debian/Ubuntu, 'netcat' on macOS via
Homebrew, or 'nmap-ncat' on RHEL) provides it.

================================================================================
EOF

if ! command -v nc >/dev/null 2>&1; then
  echo "ERROR: 'nc' (netcat) is not on PATH. Install it and re-run." >&2
  exit 1
fi

exec nc "${HOST}" "${PORT}"
