package org.hops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.DrainStatus;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.junit.Assert;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class Main extends HopsSSLTestUtils {

  private static class ClusterConfig {
    int numDataNodes = 1;
    int numNameNodes = 1;
    int nameNodePort = 8020;
    String confDir = "/tmp/hopsfs-conf";
    String ndbConfigFile = "ndb-config.properties";  // Default: bundled resource
    String dfsBaseDir = "/tmp/hopsfs-data";  // Default: temporary directory
    // Loopback control socket for kill/start commands. 0 = disabled.
    // Talk to it from another terminal with `nc localhost <port>`.
    int ctlPort = 7777;
  }

  private static ClusterConfig parseCommandLineArgs(String[] args) {
    ClusterConfig config = new ClusterConfig();

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--num-datanodes")) {
        if (i + 1 < args.length) {
          config.numDataNodes = Integer.parseInt(args[++i]);
        }
      } else if (args[i].startsWith("--num-datanodes=")) {
        config.numDataNodes = Integer.parseInt(args[i].substring("--num-datanodes=".length()));
      } else if (args[i].equals("--num-namenodes")) {
        if (i + 1 < args.length) {
          config.numNameNodes = Integer.parseInt(args[++i]);
        }
      } else if (args[i].startsWith("--num-namenodes=")) {
        config.numNameNodes = Integer.parseInt(args[i].substring("--num-namenodes=".length()));
      } else if (args[i].equals("--namenode-port")) {
        if (i + 1 < args.length) {
          config.nameNodePort = Integer.parseInt(args[++i]);
        }
      } else if (args[i].startsWith("--namenode-port=")) {
        config.nameNodePort = Integer.parseInt(args[i].substring("--namenode-port=".length()));
      } else if (args[i].equals("--conf-dir")) {
        if (i + 1 < args.length) {
          config.confDir = args[++i];
        }
      } else if (args[i].startsWith("--conf-dir=")) {
        config.confDir = args[i].substring("--conf-dir=".length());
      } else if (args[i].equals("--ndb-config")) {
        if (i + 1 < args.length) {
          config.ndbConfigFile = args[++i];
        }
      } else if (args[i].startsWith("--ndb-config=")) {
        config.ndbConfigFile = args[i].substring("--ndb-config=".length());
      } else if (args[i].equals("--dfs-base-dir")) {
        if (i + 1 < args.length) {
          config.dfsBaseDir = args[++i];
        }
      } else if (args[i].startsWith("--dfs-base-dir=")) {
        config.dfsBaseDir = args[i].substring("--dfs-base-dir=".length());
      } else if (args[i].equals("--ctl-port")) {
        if (i + 1 < args.length) {
          config.ctlPort = Integer.parseInt(args[++i]);
        }
      } else if (args[i].startsWith("--ctl-port=")) {
        config.ctlPort = Integer.parseInt(args[i].substring("--ctl-port=".length()));
      } else if (args[i].equals("--help") || args[i].equals("-h")) {
        printUsage();
        System.exit(0);
      }
    }

    return config;
  }

  private static void printUsage() {
    System.out.println("HopsFS Standalone Cluster");
    System.out.println("Usage: java -jar hopsfs-standalone.jar [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --num-datanodes=N       Number of DataNodes (default: 1)");
    System.out.println("  --num-namenodes=N       Number of NameNodes (default: 1)");
    System.out.println("  --namenode-port=N       NameNode port (default: 8020)");
    System.out.println("  --conf-dir=PATH         Configuration output directory (default: /tmp/hopsfs-conf)");
    System.out.println("  --ndb-config=FILENAME   NDB configuration fileneme (default: " + "ndb-config.properties)");
    System.out.println("  --dfs-base-dir=PATH     DFS data directory (default: /tmp/hopsfs-data)");
    System.out.println("  --ctl-port=N            Loopback control socket port (default: 7777; 0 to disable).");
    System.out.println("                          Drive interactively: nc localhost <port>");
    System.out.println("  -h, --help              Show this help message");
    System.out.println();
    System.out.println("System Properties:");
    System.out.println("  -Djava.library.path=/path/to/ndb/lib");
    System.out.println();
    System.out.println("Example:");
    System.out.println("  java -jar hopsfs-standalone.jar --num-datanodes=3 --namenode-port=8020");
  }

  public static void main(String[] args) {
    new Main().app(args);
  }


  protected String getKeyProviderURI(MiniKMS miniKMS) {
    return KMSClientProvider.SCHEME_NAME + "://" +
            miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  public void app(String[] args) {
    MiniDFSCluster cluster = null;
    MiniKMS miniKMS = null;
    final String TEST_KEY = "test_key";

    ClusterConfig config = parseCommandLineArgs(args);

    if (config.numNameNodes < 1) {
      System.err.println("Number of NameNodes must be at least 1");
      System.exit(1);
    }
    if (config.numDataNodes < 1) {
      System.err.println("Number of DataNodes must be at least 1");
      System.exit(1);
    }
    if (config.ctlPort < 0 || config.ctlPort > 65535) {
      System.err.println("--ctl-port must be in [0, 65535]");
      System.exit(1);
    }

    final int NUM_DN = config.numDataNodes;
    final int NAMENODE_PORT = config.nameNodePort;

    try {
      System.out.println("Starting HopsFS standalone cluster...");
      System.out.println("Configuration parameters:");
      System.out.println("  Number of DataNodes: " + NUM_DN);
      System.out.println("  Number of NameNodes: " + config.numNameNodes);
      System.out.println("  NameNode port: " + NAMENODE_PORT);
      System.out.println("  Configuration output directory: " + config.confDir);
      System.out.println("  NDB config file: " + config.ndbConfigFile);
      System.out.println("  DFS base directory: " + config.dfsBaseDir);
      System.out.println("  Control socket: "
          + (config.ctlPort > 0 ? "127.0.0.1:" + config.ctlPort : "disabled"));

      Configuration conf = new HdfsConfiguration();
      conf.addResource("hopsfs-site.xml");


      // ----------------------------------KMS Setup------------------------------------------------
      File kmsDir = null;
      boolean kmsEnabled = false;
      if (conf.getBoolean("enable.kms", false)) {
        File confDir = new File(config.confDir);
        kmsDir = new File(confDir, "kms");
        Assert.assertTrue(kmsDir.mkdirs());

        MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
        miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
        miniKMS.start();

        conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, getKeyProviderURI(miniKMS));
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
                getKeyProviderURI(miniKMS));
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
        // Lower the batch size for testing
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES, 2);
        kmsEnabled = true;
      }


      // ----------------------------------Cloud Setup----------------------------------------------
      String bucket = "";
      boolean cloudEnabled = false;
      if (conf.getBoolean(DFS_ENABLE_CLOUD_PERSISTENCE, DFS_ENABLE_CLOUD_PERSISTENCE_DEFAULT)) {
        cloudEnabled = true;
        String cloudProvider = conf.get(DFS_CLOUD_PROVIDER);
        if (cloudProvider == null) {
          throw new RuntimeException("Cloud persistence enabled but DFS_CLOUD_PROVIDER not set");
        }
        if (cloudProvider.equalsIgnoreCase(CloudProvider.AZURE.name())) {
          bucket = conf.get(DFSConfigKeys.AZURE_CONTAINER_KEY);
        } else if (cloudProvider.equalsIgnoreCase(CloudProvider.AWS.name())) {
          bucket = conf.get(DFSConfigKeys.S3_BUCKET_KEY);
        } else if (cloudProvider.equalsIgnoreCase(CloudProvider.GCS.name())) {
          bucket = conf.get(DFSConfigKeys.GCS_BUCKET_KEY);
        } else {
          throw new RuntimeException("Cloud provider not supported: " + cloudProvider);
        }
        if (bucket == null || bucket.isEmpty()) {
          throw new RuntimeException("Bucket not configured for cloud provider: " + cloudProvider);
        }

        CloudPersistenceProvider cloud = CloudPersistenceProviderFactory.getCloudClient(conf);
        cloud.deleteAllBuckets(bucket);
        cloud.createBucket(bucket.toLowerCase());
        cloud.shutdown();
      }

      // ----------------------------------Storage dirs---------------------------------------------
      conf.setStrings(DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE, config.ndbConfigFile);
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, config.dfsBaseDir);
      conf.set(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY, System.getProperty("user.name"));

      // ----------------------------------SSL configuration----------------------------------------
      String cryptoDir = "";
      boolean sslEnabled = false;
      if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, false)) {
        File confDir = new File(config.confDir);
        cryptoDir = (new File(confDir, "certs")).toString();
        prepareCryptoMaterial(cryptoDir);

        conf.setEnum("hops.tls.rpc-acl-auth-mode", io.hops.security.HopsX509Authenticator.AUTH_MODE.NONE); // Disable auth checks for proxy users
        setCryptoConfig(conf, cryptoDir);
        sslEnabled = true;
      }

      // ----------------------------------Enable user impersonation for this user------------------
      String currentUser = System.getProperty("user.name");
      conf.set("hadoop.proxyuser." + currentUser + ".hosts", "*");
      conf.set("hadoop.proxyuser." + currentUser + ".groups", "*");

      // ----------------------------------Fast DN dead detection for ctl-driven kills--------------
      // When the control socket is on, operators will be killing DNs by
      // hand and expect the NN to notice promptly. The default
      // 10.5-minute dead-detection window (2 * recheck + 10 * heartbeat)
      // makes that feel broken. Tighten to ~20 s:
      //   dead = 2 * 5000 ms + 10 * 1 s = 20 s.
      // Non-control runs keep stock Hadoop heartbeat behavior.
      if (config.ctlPort > 0) {
        conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1L);
        conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 5000);
      }

      System.out.println("Building MiniDFSCluster...");
      MiniDFSCluster.Builder clusterBuilder = new MiniDFSCluster.Builder(conf)
              .numDataNodes(NUM_DN);
      if (cloudEnabled) {
        clusterBuilder.storageTypes(CloudTestHelper.genStorageTypes(NUM_DN));
      }

      // Set up NN topology with ports: first NN uses NAMENODE_PORT, others use 0
      int[] ipcPorts = new int[config.numNameNodes];
      ipcPorts[0] = NAMENODE_PORT;
      clusterBuilder.nnTopology(MiniDFSNNTopology.simpleHOPSTopology(config.numNameNodes, ipcPorts));

      clusterBuilder = clusterBuilder.format(true);
      cluster = clusterBuilder.build();

      System.out.println("Cluster started successfully!");

      // ----------------------------------Setup KMS------------------------------------------------
      if (kmsEnabled) {
        final HdfsAdmin dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
        DFSTestUtil.createKey(TEST_KEY, cluster, conf);

        final Path zone = new Path("/");
        dfsAdmin.createEncryptionZone(zone, TEST_KEY);
      }

      // ----------------------------------Get file system clients----------------------------------
      FileSystem fs = cluster.getFileSystem(0);
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
              .newInstance(fs.getUri(), fs.getConf());

      // ----------------------------------Set storage policy---------------------------------------
      if (cloudEnabled) {
        dfs.setStoragePolicy(new Path("/"), "CLOUD");
      } else {
        dfs.setStoragePolicy(new Path("/"), "HOT");
      }

      // ----------------------------------Create additional user and group -------------------------
      String addUser = conf.get("hopsfs.additional.user", "");
      String addGroup = conf.get("hopsfs.additional.group", "");
      if (addUser != null && !addUser.isEmpty() && addGroup != null && !addGroup.isEmpty()) {
        try {
          dfs.addUser(addUser);
          dfs.addGroup(addGroup);
          dfs.addUserToGroup(addUser, addGroup);
          System.out.println("Added user '" + addUser + "' to group '" + addGroup + "'");
        } catch (Exception e) {
          System.err.println("Warning: Failed to add user/group: " + e.getMessage());
        }
      }

      // ----------------------------------Create sample data---------------------------------------
      if (conf.getBoolean("create.test.data", false)) {

        dfs.mkdirs(new Path("/_test"), new FsPermission(0777));
        dfs.setPermission(new Path("/_test"), new FsPermission(0777));

        InputStream in = Main.class.getClassLoader().getResourceAsStream("foo.txt");
        if (in == null) {
          throw new RuntimeException("Resource not found: foo.txt");
        }
        FSDataOutputStream out = dfs.create(new Path("/_test/foo.txt"));
        IOUtils.copyBytes(in, out, 1024);
        in.close();
        out.close();

        in = Main.class.getClassLoader().getResourceAsStream("mobydick.txt");
        if (in == null) {
          throw new RuntimeException("Resource not found: mobydick.txt");
        }
        out = dfs.create(new Path("/_test/mobydick.txt"),
                false, 1024, (short) 3, 1024 * 1024);
        IOUtils.copyBytes(in, out, 1024);
        in.close();
        out.close();

        // hopsfs-go-client integration tests connect as user "gohdfs1" and
        // expect /_test/foo.txt and /_test/mobydick.txt to be readable by
        // that user. Create the user/group and chown the fixtures so the
        // Go test suite (which hardcodes "gohdfs1") can read them without
        // permission errors.
        final String goUser = "gohdfs1";
        final String goGroup = "gohdfs1";
        try {
          dfs.addUser(goUser);
          dfs.addGroup(goGroup);
          dfs.addUserToGroup(goUser, goGroup);
          System.out.println("Added user '" + goUser + "' to group '" + goGroup + "'");
        } catch (Exception e) {
          System.err.println("Warning: Failed to add gohdfs1 user/group: " + e.getMessage());
        }
        dfs.setOwner(new Path("/_test/foo.txt"), goUser, goGroup);
        dfs.setOwner(new Path("/_test/mobydick.txt"), goUser, goGroup);
        dfs.setPermission(new Path("/_test/foo.txt"), new FsPermission(0644));
        dfs.setPermission(new Path("/_test/mobydick.txt"), new FsPermission(0644));

        // Create a folder only accessible by testuser (if configured)
        if (addUser != null && !addUser.isEmpty()) {
          dfs.mkdirs(new Path("/_test/testuser_only"), new FsPermission(0700));
          dfs.setOwner(new Path("/_test/testuser_only"), addUser, addGroup);
        }
      }

      dfs.close();

      // Write HopsFS configuration files
      writeHopsFSConfig(cluster, config.confDir);

      System.out.println("================================================================================");
      System.out.println("NameNode address: " + cluster.getNameNode(0).getHostAndPort());
      System.out.println("HTTP address: " + cluster.getNameNode(0).getHttpAddress());
      System.out.println("Configuration written to: " + config.confDir);
      System.out.println("Cloud Enabled: " + cloudEnabled);
      if (cloudEnabled) {
        // Async cloud upload tunables (HOPSFS-345). Values are read from
        // hopsfs-site.xml or fall back to the Java defaults.
        boolean asyncEnabled = conf.getBoolean(
            DFS_CLOUD_ASYNC_UPLOAD_ENABLED_KEY,
            DFS_CLOUD_ASYNC_UPLOAD_ENABLED_DEFAULT);
        System.out.println("  " + DFS_CLOUD_ASYNC_UPLOAD_ENABLED_KEY
            + " = " + asyncEnabled);
        if (asyncEnabled) {
          System.out.println("  " + DFS_CLOUD_DN_ASYNC_UPLOAD_THREADS_KEY
              + " = " + conf.getInt(DFS_CLOUD_DN_ASYNC_UPLOAD_THREADS_KEY,
                  DFS_CLOUD_DN_ASYNC_UPLOAD_THREADS_DEFAULT));
          System.out.println("  " + DFS_CLOUD_DN_ASYNC_UPLOAD_QUEUE_CAPACITY_KEY
              + " = " + conf.getInt(DFS_CLOUD_DN_ASYNC_UPLOAD_QUEUE_CAPACITY_KEY,
                  DFS_CLOUD_DN_ASYNC_UPLOAD_QUEUE_CAPACITY_DEFAULT));
          System.out.println("  " + DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_COUNT_KEY
              + " = " + conf.getInt(DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_COUNT_KEY,
                  DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_COUNT_DEFAULT));
          System.out.println("  " + DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_INTERVAL_MS_KEY
              + " = " + conf.getLong(DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_INTERVAL_MS_KEY,
                  DFS_CLOUD_DN_ASYNC_UPLOAD_RETRY_INTERVAL_MS_DEFAULT) + "ms");
          // The cache-delete-activation percentage gates both the cache
          // eviction trigger AND the async-upload sync-fallback gate after
          // the HOPSFS-345 cleanup that dropped the redundant
          // dfs.cloud.dn.async.upload.disk.threshold.percent key.
          System.out.println("  " + DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY
              + " = " + conf.getInt(DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY,
                  DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_DEFAULT) + "%");
        }
      }
      System.out.println("SSL Enabled: " + sslEnabled);
      if (sslEnabled) {
        System.out.println("SSL Crypt Dir: " + cryptoDir);
      }
      System.out.println("KMS Enabled: " + kmsEnabled);
      if (kmsEnabled) {
        System.out.println("KMS Provider: " + getKeyProviderURI(miniKMS));
      }
      System.out.println("================================================================================");
      System.out.println("HopsFS cluster is running!");
      System.out.println("Press Ctrl+C to shutdown...");

      startCtlServer(cluster, config);

      // Keep the cluster running
      Thread.sleep(Long.MAX_VALUE);

    } catch (InterruptedException e) {
      System.out.println("Cluster interrupted, shutting down...");
    } catch (Exception e) {
      System.err.println("Error running HopsFS standalone cluster: " + e.getMessage());
      e.printStackTrace();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static void startCtlServer(MiniDFSCluster cluster, ClusterConfig config) {
    if (config.ctlPort <= 0) {
      return;
    }
    final ServerSocket ss;
    try {
      ss = new ServerSocket(config.ctlPort, 0, InetAddress.getLoopbackAddress());
    } catch (IOException e) {
      System.err.println("[CTL] Failed to bind control socket on port "
          + config.ctlPort + ": " + e.getMessage());
      return;
    }
    final Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns = new HashMap<>();
    Thread t = new Thread(() -> acceptLoop(ss, cluster, stoppedDns), "hopsfs-ctl");
    t.setDaemon(true);
    t.start();
    System.out.println("[CTL] Control socket listening on 127.0.0.1:"
        + ss.getLocalPort() + ". From another terminal: nc localhost "
        + ss.getLocalPort());
  }

  private static void acceptLoop(ServerSocket ss, MiniDFSCluster cluster,
                                 Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns) {
    while (!Thread.currentThread().isInterrupted()) {
      try (Socket client = ss.accept();
           BufferedReader in = new BufferedReader(
               new InputStreamReader(client.getInputStream()));
           PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {
        out.println("hopsfs-ctl ready. Type 'help'.");
        String line;
        while ((line = in.readLine()) != null) {
          String trimmed = line.trim();
          if (trimmed.isEmpty()) {
            continue;
          }
          if (trimmed.equalsIgnoreCase("quit") || trimmed.equalsIgnoreCase("exit")) {
            out.println("bye");
            break;
          }
          try {
            out.println(handleCtlCommand(cluster, trimmed, stoppedDns));
          } catch (Exception e) {
            out.println("ERROR: " + e.getMessage());
          }
        }
      } catch (IOException e) {
        // Per-connection failure; loop and accept again.
      }
    }
  }

  private static String handleCtlCommand(MiniDFSCluster cluster, String line,
      Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns) throws IOException {
    String[] p = line.split("\\s+");
    String cmd = p[0].toLowerCase();
    switch (cmd) {
      case "help":
        return helpText();
      case "list":
        return listText(cluster, stoppedDns);
      case "kill":
      case "stop":
        return doKill(cluster, p, stoppedDns);
      case "start":
      case "restart":
        return doStart(cluster, p, stoppedDns);
      default:
        return "ERROR: unknown command '" + cmd + "'. Type 'help'.";
    }
  }

  private static String doKill(MiniDFSCluster cluster, String[] p,
      Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns) throws IOException {
    if (p.length != 3) {
      return "ERROR: usage: kill {dn|nn} <idx>";
    }
    String role = p[1].toLowerCase();
    int idx;
    try {
      idx = Integer.parseInt(p[2]);
    } catch (NumberFormatException nfe) {
      return "ERROR: idx must be an integer";
    }
    switch (role) {
      case "dn":
      case "datanode": {
        if (stoppedDns.containsKey(idx)) {
          return "DN " + idx + " is already stopped";
        }
        List<DataNode> dns = cluster.getDataNodes();
        if (idx < 0 || idx >= dns.size()) {
          return "ERROR: no DN at index " + idx;
        }
        DataNode dn = dns.get(idx);
        StringBuilder out = new StringBuilder();
        // When async cloud upload is on, drain pending uploads first so
        // the DN gets the same graceful-shutdown treatment the Helm
        // preStop hook applies in production.
        boolean asyncEnabled = dn.getConf().getBoolean(
            DFS_CLOUD_ASYNC_UPLOAD_ENABLED_KEY,
            DFS_CLOUD_ASYNC_UPLOAD_ENABLED_DEFAULT);
        if (asyncEnabled) {
          out.append("Async upload on; draining DN ").append(idx).append("...\n");
          System.out.println("[CTL] Draining DN " + idx + "...");
          try {
            DrainStatus status = dn.drainAndSuspend(600L);
            out.append("Drain: ").append(status).append("\n");
            System.out.println("[CTL] DN " + idx + " drain: " + status);
          } catch (Exception e) {
            out.append("Drain failed: ").append(e.getMessage())
                .append(" (proceeding with stop)\n");
            System.err.println("[CTL] DN " + idx + " drain failed: " + e.getMessage());
          }
        }
        MiniDFSCluster.DataNodeProperties props = cluster.stopDataNode(idx);
        if (props == null) {
          return out.append("ERROR: stop returned null for index ").append(idx).toString();
        }
        stoppedDns.put(idx, props);
        System.out.println("[CTL] DN " + idx + " stopped");
        return out.append("DN ").append(idx).append(" stopped").toString();
      }
      case "nn":
      case "namenode": {
        cluster.shutdownNameNode(idx);
        System.out.println("[CTL] NN " + idx + " stopped");
        return "NN " + idx + " stopped";
      }
      default:
        return "ERROR: unknown role '" + role + "' (use 'dn' or 'nn')";
    }
  }

  private static String doStart(MiniDFSCluster cluster, String[] p,
      Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns) throws IOException {
    if (p.length != 3) {
      return "ERROR: usage: start {dn|nn} <idx>";
    }
    String role = p[1].toLowerCase();
    int idx;
    try {
      idx = Integer.parseInt(p[2]);
    } catch (NumberFormatException nfe) {
      return "ERROR: idx must be an integer";
    }
    switch (role) {
      case "dn":
      case "datanode": {
        MiniDFSCluster.DataNodeProperties props = stoppedDns.remove(idx);
        if (props == null) {
          return "ERROR: DN " + idx + " is not in stopped state";
        }
        boolean ok = cluster.restartDataNode(props, false);
        System.out.println("[CTL] DN " + idx + (ok ? " restarted" : " restart failed"));
        return ok ? "DN " + idx + " restarted" : "ERROR: restart failed";
      }
      case "nn":
      case "namenode": {
        cluster.restartNameNode(idx, true);
        System.out.println("[CTL] NN " + idx + " restarted");
        return "NN " + idx + " restarted";
      }
      default:
        return "ERROR: unknown role '" + role + "' (use 'dn' or 'nn')";
    }
  }

  private static String listText(MiniDFSCluster cluster,
      Map<Integer, MiniDFSCluster.DataNodeProperties> stoppedDns) {
    StringBuilder sb = new StringBuilder();
    int numNn = cluster.getNumNameNodes();
    sb.append("NameNodes (").append(numNn).append("):\n");
    for (int i = 0; i < numNn; i++) {
      String addr;
      try {
        addr = cluster.getNameNode(i).getHostAndPort();
      } catch (Exception e) {
        addr = "(stopped)";
      }
      sb.append("  [").append(i).append("] ").append(addr).append("\n");
    }
    int numDnRunning = cluster.getDataNodes().size();
    sb.append("DataNodes (").append(numDnRunning).append(" running");
    if (!stoppedDns.isEmpty()) {
      sb.append(", ").append(stoppedDns.size()).append(" stopped at indices ")
          .append(stoppedDns.keySet());
    }
    sb.append(")");
    return sb.toString();
  }

  private static String helpText() {
    return String.join("\n",
        "Commands:",
        "  help                Show this help",
        "  list                Show NN/DN status",
        "  kill {dn|nn} <idx>  Stop a node (keeps DN data for later start)",
        "  start {dn|nn} <idx> Start a previously stopped node",
        "  quit                Close this connection (cluster keeps running)");
  }

  private static void writeHopsFSConfig(MiniDFSCluster cluster, String confDir) throws IOException {
    File file = new File(confDir);
    file.mkdirs();

    cluster.getConfiguration(0).set("fs.defaultFS",
            "hdfs://" + cluster.getNameNode(0).getHostAndPort());

    FileOutputStream os = new FileOutputStream(confDir + "/hdfs-site.xml");
    try {
      cluster.getConfiguration(0).writeXml(os);
    } finally {
      os.close();
    }

    FileWriter writer = new FileWriter(confDir + "/hopsfs-uri.txt");
    try {
      writer.write(cluster.getNameNode(0).getHostAndPort());
    } finally {
      writer.close();
    }

    System.out.println("Configuration files written to " + confDir);
  }
}