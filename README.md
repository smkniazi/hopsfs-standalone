# HopsFS Standalone

A standalone HopsFS MiniDFSCluster for testing and development.

## Prerequisites

- Java 8 or higher
- Maven 3.x
- Access to HopsFS Maven repositories (configured in your Maven settings.xml)

## Project Structure

```
src/main/java/org/hops/Main.java              - Standalone cluster runner
```

## Building

Build a fat JAR with all dependencies:

```bash
mvn clean package -DskipTests
```

This creates a standalone JAR at `target/hopsfs-standalone-1.0-SNAPSHOT.jar` that can run on any machine with Java.

## Running

```bash
java -jar target/hopsfs-standalone-1.0-SNAPSHOT.jar
```

Or with options:
```bash
java -jar target/hopsfs-standalone-1.0-SNAPSHOT.jar --num-datanodes=3 --namenode-port=8020
```

## Command-Line Options

```
  --num-datanodes=N       Number of DataNodes (default: 1)
  --num-namenodes=N       Number of NameNodes (default: 1)
  --namenode-port=N       NameNode port (default: 8020)
  --conf-dir=PATH         Configuration output directory (default: /tmp/hopsfs-conf)
  --ndb-config=FILENAME   NDB configuration filename on classpath (default: ndb-config.properties)
  --dfs-base-dir=PATH     DFS data directory (default: /tmp/hopsfs-data)
  --ctl-port=N            Loopback control socket port (default: 7777; 0 to disable)
  -h, --help              Show this help message
```

### Interactive control socket (kill / start nodes by hand)

By default the cluster opens a plain-text control socket on
`127.0.0.1:7777`. Drive it from a separate terminal so the cluster's
NN/DN log stream stays on its own console:

```bash
# Interactive session
nc localhost 7777
hopsfs-ctl ready. Type 'help'.
help
list
kill dn 0
start dn 0
kill nn 1
start nn 1
quit

# One-shot
echo "list" | nc localhost 7777
echo "kill dn 0" | nc localhost 7777
```

Commands:

- `help` — show commands
- `list` — show NN/DN status (which are running, which are stopped)
- `kill {dn|nn} <idx>` — stop a node. DN stop preserves the data dir
  and port info so `start dn <idx>` brings the same instance back.
- `start {dn|nn} <idx>` — start a previously stopped node.
- `quit` — close just this connection; the cluster keeps running.

When the control socket is enabled (the default), the cluster overrides
the heartbeat tunables so the NameNode expires a dead DataNode in
~20 seconds (`dfs.heartbeat.interval=1s`,
`dfs.namenode.heartbeat.recheck-interval=5000ms`). Without this you'd
wait ~10.5 minutes after `kill dn` before the NN noticed. Pass
`--ctl-port=0` to disable the socket and keep stock Hadoop heartbeat
behavior.

The socket binds to `127.0.0.1` only — not reachable from outside the
host. It's intended for local development and testing.

### Helper scripts

Three wrappers under `scripts/` save you the typing:

| Script | Purpose |
|---|---|
| `scripts/hopsfs-ctl.sh` | Interactive launcher. Prints a usage banner with all supported commands, then exec's `nc localhost 7777`. Use this for ad-hoc `kill`/`start`/`list`. |
| `scripts/hopsfs-churn-dn.sh [interval-secs] [dn-idx]` | Periodic DataNode churn loop. Defaults: 60 s phases, DN 0. Cycles `kill dn` → sleep → `start dn` → sleep. When async cloud upload is on, the runner drains the DN before stopping it. |
| `scripts/hopsfs-churn-nn.sh [interval-secs] [nn-idx]` | Periodic NameNode churn loop. Defaults: 60 s phases, NN 0. Same shape as the DN variant; no drain step (drain is DN-only). |

Each script prints a detailed banner — target host/port, what the loop
does, prerequisites, env overrides — before doing anything network. All
three honor `HOPSFS_CTL_HOST` and `HOPSFS_CTL_PORT` so you can point
them at a non-default port.

Examples:

```bash
# Interactive
./scripts/hopsfs-ctl.sh

# DN0 dies every 60 s for 60 s, then comes back; repeat forever
./scripts/hopsfs-churn-dn.sh

# 30-second phases, target DN1
./scripts/hopsfs-churn-dn.sh 30 1

# NN1 churn at 30-second phases
./scripts/hopsfs-churn-nn.sh 30 1
```

For NN churn you need `--num-namenodes >= 2` so clients can fail over;
for DN churn you need `--num-datanodes >= 2` so writes can still find a
healthy target while the churned DN is down.

## NDB Configuration

The `--ndb-config` option specifies a filename that is loaded from the classpath. To use a custom NDB configuration file, add the directory containing your config file to the classpath:

```bash
java -cp "/etc/hopsfs:target/hopsfs-standalone-1.0-SNAPSHOT.jar" org.hops.Main \
     --ndb-config=ndb-config.properties
```

In this example, `/etc/hopsfs` is added to the classpath, so the file `/etc/hopsfs/ndb-config.properties` will be found when specifying `--ndb-config=ndb-config.properties`.

You can also override individual settings with system properties:

```bash
java -Dcom.mysql.clusterj.connectstring=ndb-host:1186 \
     -Dcom.mysql.clusterj.database=hops \
     -Dio.hops.metadata.ndb.mysqlserver.host=mysql-host \
     -Dio.hops.metadata.ndb.mysqlserver.port=3306 \
     -Djava.library.path=/path/to/ndb/lib \
     -cp "target/hopsfs-standalone-1.0-SNAPSHOT.jar" org.hops.Main
```

See `src/main/resources/ndb-config.properties` for all available options.


## Output Files

After starting, configuration files are written to `--conf-dir` (default: `/tmp/hopsfs-conf`):
- `hdfs-site.xml` - HDFS configuration for client applications
- `hopsfs-uri.txt` - NameNode hostname:port

## Examples

### Quick Start
```bash
java -jar target/hopsfs-standalone-1.0-SNAPSHOT.jar
```
Starts a cluster with 1 NameNode (port 8020), 1 DataNode, configs in `/tmp/hopsfs-conf/`, data in `/tmp/hopsfs-data/`.

### Custom Cluster
```bash
java -cp "target/hopsfs-standalone-1.0-SNAPSHOT.jar" org.hops.Main \
     --num-namenodes=2 \
     --num-datanodes=3 \
     --namenode-port=9000 \
     --conf-dir=/var/lib/hopsfs/config \
     --dfs-base-dir=/var/lib/hopsfs/data
```

### With Custom NDB Config
```bash
java -cp "/etc/hopsfs:target/hopsfs-standalone-1.0-SNAPSHOT.jar" org.hops.Main \
     --ndb-config=ndb-config.properties \
     --num-datanodes=2
```

## Stopping

Press `Ctrl+C` to gracefully shutdown the cluster.
