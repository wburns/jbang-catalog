///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 17+
//DEPS org.infinispan:infinispan-bom:15.2.0-SNAPSHOT@pom
//DEPS info.picocli:picocli:4.6.3
//DEPS org.jline:jline-console-ui:3.26.3
//DEPS org.infinispan:infinispan-server-testdriver-core
//DEPS org.jboss:jandex:2.4.3.Final

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;

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
   String protocol;

   @Option(names = "-d", defaultValue = "false", description = "Enables dumping server log files to the local running directory")
   boolean dumpLogsOnException;

   @Option(names = "-w", defaultValue = "false", description = "If an exception occurs the script will wait for input before continuing. Allows for inspecting images before deletion.")
   boolean waitOnException;

   @Parameters(index = "0", description = "Old version to migrate from. Supports image name at quay.io/infinispan/server, any local image or a server directory")
   private String versionFrom;

   @Parameters(index = "1", description = "New version to migrate to. Supports image name at quay.io/infinispan/server, any local image or a server directory")
   private String versionTo;

   private String site1Name = "site1";
   private String site2Name = "site2";

   private ContainerInfinispanServerDriver fromDriver;
   private ContainerInfinispanServerDriver toDriver;

   private String toImageCreated;
   private String fromImageCreated;

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

      try {
         System.out.println("Starting " + nodeCount + " node to version " + versionFrom);
         fromDriver = startNode(false, nodeCount, nodeCount, site1Name, versionFrom);

         TestUser user = TestUser.ADMIN;

         String hotrodURI = "hotrod://" + user.getUser() + ":" + user.getPassword() + "@" + fromDriver.getServerAddress(0).getHostAddress() + ":11222";
         System.out.println("Creating RCM with uri: " + hotrodURI);
         try (RemoteCacheManager manager = new RemoteCacheManager(hotrodURI)) {

            System.out.println("Adding cache");
            RemoteCache<String, String> cache = manager.administration().createCache("rolling-upgrade", new StringConfiguration("<replicated-cache></replicated-cache>"));

            System.out.println("Inserting value");
            cache.put("foo", "bar");

            System.out.println("Servers are: " + Arrays.toString(manager.getServers()));

            if (manager.getServers().length != nodeCount) {
               throw new IllegalStateException("Node count does not match: " + Arrays.toString(manager.getServers()) + " didn't have " + nodeCount + " members");
            }

            for (int i = 0; i < nodeCount; ++i) {

               // TODO: disable state transfer

               System.out.println("Starting 1 node to version " + versionTo);
               if (toDriver == null) {
                  toDriver = startNode(true, 1, nodeCount + 1, site1Name, versionTo);
               } else {
                  toDriver.startAdditionalServer(nodeCount + 1);
               }

               if (!checkCacheServers(cache, nodeCount + 1)) {
                  System.out.println("Servers are only: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not cluster within 30 seconds, assuming error");
               }

               System.out.println("Shutting down 1 node from version: " + versionFrom);
               fromDriver.stop(nodeCount - i - 1);

               if (!checkCacheServers(cache, nodeCount)) {
                  System.out.println("Servers are: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not shut down properly within 30 seconds, assuming error");
               }
            }
         }
      } catch (Throwable t) {
         if (dumpLogsOnException) {
            if (fromDriver != null) {
               for (int i = 0; i < fromDriver.serverCount(); ++i) {
                  System.out.println("Writing server log files from " + versionFrom + " to " + i);
                  fromDriver.syncFilesFromServer(i, "log");
               }
            }

            if (toDriver != null) {
               for (int i = 0; i < toDriver.serverCount(); ++i) {
                  System.out.println("Writing server log files from " + versionTo + " to " + i);
                  toDriver.syncFilesFromServer(i, "log");
               }
            }
         }
         if (waitOnException) {
            System.out.println("Waiting for user to press enter to shutdown server nodes");
            System.in.read();
         }
         throw t;
      } finally {
         if (fromDriver != null) {
            fromDriver.stop(versionFrom);
         }
         if (toDriver != null) {
            toDriver.stop(versionTo);
         }
         if (toImageCreated != null) {
            ContainerInfinispanServerDriver.cleanup(toImageCreated);
         }
         if (fromImageCreated != null) {
            ContainerInfinispanServerDriver.cleanup(fromImageCreated);
         }
      }
      return 0;
   }

   private boolean checkCacheServers(RemoteCache<String, String> cache, int expectedCount) throws InterruptedException {
      long begin = System.nanoTime();
      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < 30) {
         System.out.println("Attempting remote call to ensure cluster formed properly");
         String value = cache.get("foo");
         if (!value.equals("bar")) {
            throw new IllegalStateException("Remote cache returned " + value + " instead of bar");
         }

         Set<SocketAddress> servers = cache.getCacheTopologyInfo().getSegmentsPerServer().keySet();
         if (servers.size() == expectedCount) {
            System.out.println("Servers are: " + servers);
            return true;
         }
         Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
      System.out.println("Improper shutdown detected, servers are: " + cache.getCacheTopologyInfo().getSegmentsPerServer().keySet());
      return false;
   }

   private ContainerInfinispanServerDriver startNode(boolean toOrFrom, int nodeCount, int expectedCount, String clusterName,
                                                     String versionToUse) {
      ServerConfigBuilder builder = new ServerConfigBuilder("infinispan.xml", true);
      builder.runMode(ServerRunMode.CONTAINER);
      builder.numServers(nodeCount);
      builder.expectedServers(expectedCount);
      builder.clusterName(clusterName);
      builder.property(Server.INFINISPAN_CLUSTER_STACK, protocol);
      builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT, "true");

      if (versionToUse.startsWith("image://")) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME, versionToUse.substring("image://".length()));
      } else if (versionToUse.startsWith("file://")) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR, versionToUse.substring("file://".length()));
         // For simplicity trim down to the directory name for the rest of the test
         versionToUse = Path.of(versionToUse).getFileName().toString();

         String imageName;
         if (toOrFrom) {
            imageName = toImageCreated = ContainerInfinispanServerDriver.SNAPSHOT_IMAGE + "-to";
         } else {
            imageName = fromImageCreated = ContainerInfinispanServerDriver.SNAPSHOT_IMAGE + "-from";
         }
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_SNAPSHOT_IMAGE_NAME, imageName);
      } else {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_VERSION, versionToUse);
      }
      InfinispanServerTestConfiguration config = builder.createServerTestConfiguration();

      ContainerInfinispanServerDriver driver = (ContainerInfinispanServerDriver) ServerRunMode.CONTAINER.newDriver(config);
      driver.prepare(versionToUse);
      driver.start(versionToUse);
      return driver;
   }
}

