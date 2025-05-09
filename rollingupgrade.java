///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17+
//DEPS org.infinispan:infinispan-bom:16.0.0-SNAPSHOT@pom
//DEPS info.picocli:picocli:4.6.3
//DEPS org.jline:jline-console-ui:3.26.3
//DEPS org.infinispan:infinispan-server-testdriver-core
//DEPS org.jboss:jandex:2.4.3.Final

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.Callable;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "infinispan", mixinStandardHelpOptions = true, version = "infinispan 16.0",
      description = "infinispan made with jbang")
class rollingupgrade implements Callable<Integer> {

   @Option(names = "-n", defaultValue = "3", description = "How many nodes to start up and roll over (default: 3)")
   int nodeCount;

   @Option(names = "-x", defaultValue = "false", description = "Enables xsite rolling upgrade")
   boolean xsite;

   @Option(names = "-p", defaultValue = "tcp", description =  "Sets the JGroups protocol (default: tcp)")
   String jgroupsProtocol;

   @Option(names = "-d", defaultValue = "false", description = "Enables dumping server log files to the local running directory")
   boolean dumpLogsOnException;

   @Option(names = "-w", defaultValue = "false", description = "If an exception occurs the script will wait for input before continuing. Allows for inspecting images before deletion.")
   boolean waitOnException;

   @Parameters(index = "0", description = "Old version to migrate from. Supports image name at quay.io/infinispan/server, any local image (prepended by image://) or a server directory (prepended by file://)")
   String versionFrom;

   @Parameters(index = "1", description = "New version to migrate to. Supports image name at quay.io/infinispan/server, any local image (prepended by image://) or a server directory (prepended by file://)")
   String versionTo;

   @Option(names = "-m", defaultValue = "false", description = "If the servers should not use a shared mounted data directory beetween versions")
   boolean noDataMount;

   public static void main(String... args) {
      int exitCode = new CommandLine(new rollingupgrade()).execute(args);
      System.exit(exitCode);
   }

   @Override
   public Integer call() throws InterruptedException, IOException { // your business logic goes here...
      if (versionTo.equals(versionFrom)) {
         throw new IllegalArgumentException("to and from version " + versionTo + " cannot be the same!");
      }
      System.out.println("Rolling upgrade from: " + versionFrom + " to " + versionTo + " with " + nodeCount + " nodes");

      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(versionFrom, versionTo);

      builder = builder.nodeCount(nodeCount)
            .jgroupsProtocol(jgroupsProtocol)
            .xSite(xsite)
            .sharedDataMount(!noDataMount);

      if (waitOnException || dumpLogsOnException) {
         builder = builder.exceptionHandler((t, uh) -> {
            if (dumpLogsOnException) {
               var fromDriver = uh.getFromDriver();
               if (fromDriver != null) {
                  for (int i = 0; i < fromDriver.serverCount(); ++i) {
                     System.out.println("Writing server log files from " + versionFrom + " to " + i);
                     fromDriver.syncFilesFromServer(i, "log");
                  }
               }

               var toDriver = uh.getToDriver();
               if (toDriver != null) {
                  for (int i = 0; i < toDriver.serverCount(); ++i) {
                     System.out.println("Writing server log files from " + versionTo + " to " + i);
                     toDriver.syncFilesFromServer(i, "log");
                  }
               }
            }
            if (waitOnException) {
               t.printStackTrace();
               System.out.println("Attempting new connection to server to see status");
               try {
                  RemoteCacheManager manager = uh.createRemoteCacheManager();
                  RemoteCache<?, ?> cache = manager.getCache("rolling-upgrade");
                  cache.get("foo");
                  Set<SocketAddress> servers = cache.getCacheTopologyInfo().getSegmentsPerServer().keySet();
                  System.out.println("Created new cache manager with client and servers found were: " + servers);
               } catch (Throwable innerT) {
                  System.out.println("Unable to create CacheManager and cache to verify members");
                  innerT.printStackTrace();
               }
               System.out.println("Waiting for user to press enter to shutdown server nodes");
               try {
                  System.in.read();
               } catch (IOException e) {
                  System.err.println("Ignoring IOException " + e);
               }
            }
         });
      }

      RollingUpgradeConfiguration config = builder.build();

      RollingUpgradeHandler.performUpgrade(config);

      System.out.println("Upgrade from " + versionFrom + " to " + versionTo + " completed successfully!");

      return 0;
   }
}

