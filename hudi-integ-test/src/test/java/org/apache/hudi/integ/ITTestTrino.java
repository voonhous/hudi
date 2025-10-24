/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ITTestTrino {

  private static final Logger LOG = LoggerFactory.getLogger(ITTestTrino.class);

  private static final Network network = Network.newNetwork();

  private static final String TRINO_IMAGE = "trinodb/trino:472";

  @Container
  private static final GenericContainer<?> trinoCoordinator = new GenericContainer<>(TRINO_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("trino-coordinator") // Crucial for discovery
      .withExposedPorts(8080)
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("trino/coordinator/"),
          "/etc/trino/"
      )
      // Wait until the UI is accessible, indicating the cluster is ready
      .waitingFor(Wait.forHttp("/ui/").forStatusCode(200))
      // Wait for appropriate logs
      .waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1));

  @Container
  private static final GenericContainer<?> trinoWorker1 = createTrinoWorker("worker-1");

  @Container
  private static final GenericContainer<?> trinoWorker2 = createTrinoWorker("worker-2");


  /**
   * Helper method to create and configure a Trino worker container.
   */
  private static GenericContainer<?> createTrinoWorker(String workerId) {
    LOG.info("Creating trino worker " + workerId);

    // Create a unique node.properties file for each worker
    String nodePropsContent = String.format(
        "node.environment=test\nnode.id=%s\nnode.data-dir=/data/trino",
        workerId + "-" + UUID.randomUUID() // Ensure ID is always unique
    );

    try {
      // Define the desired permissions (rw-r--r-- which is 644), permissions will be copied over to container
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-r--r--");
      FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);

      // Create a temporary file on the host machine
      Path tempNodeProperties = Files.createTempFile(workerId + "-node", ".properties", attr);

      // Write dynamic configuration string to this temp file
      Files.write(tempNodeProperties, nodePropsContent.getBytes(StandardCharsets.UTF_8));

      // This ensures the temporary file is cleaned up when the test process exits
      tempNodeProperties.toFile().deleteOnExit();

      return new GenericContainer<>(TRINO_IMAGE)
          .withNetwork(network)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("trino/worker/config.properties"),
              "/etc/trino/config.properties"
          )
          .withCopyFileToContainer(
              MountableFile.forHostPath(tempNodeProperties),
              "/etc/trino/node.properties"
          )
          .dependsOn(trinoCoordinator)
          .waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1));

    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary config for worker " + workerId, e);
    }
  }

  @Test
  void testTrinoClusterIsUpAndQueryable() throws Exception {
    String jdbcUrl = String.format("jdbc:trino://%s:%d",
        trinoCoordinator.getHost(),
        trinoCoordinator.getMappedPort(8080));

    Thread.sleep(10000);

    LOG.info("Probing available nodes");

    final int maxRetries = 3;
    int attempt = 0;
    // Expect 3 active nodes: 1 coordinator and 2 workers
    int expectedNodeCount = 3;

    // Use a try-with-resources block to ensure the connection is closed
    try (Connection connection = DriverManager.getConnection(jdbcUrl, "test", null);
        Statement statement = connection.createStatement()) {

      int activeNodeCount = 0;
      while (attempt < maxRetries) {
        activeNodeCount = 0;
        try {
          // Query the system table to see the nodes in the cluster
          ResultSet rs = statement.executeQuery("SELECT state FROM system.runtime.nodes");

          while (rs.next()) {
            if ("active".equals(rs.getString("state"))) {
              activeNodeCount++;
            }
          }

          assertEquals(expectedNodeCount, activeNodeCount, "Should be three active nodes in the cluster.");
          LOG.info("Successfully queried Trino cluster. Found {} active nodes.", activeNodeCount);
          return;
        } catch (Exception e) {
          LOG.error("Validate failed, waiting for the next loop...", e);
        } catch (AssertionError ignored) {
          LOG.error("Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
              expectedNodeCount, activeNodeCount);
        }
        attempt++;
        Thread.sleep(2000);
      }
      assertEquals(expectedNodeCount, activeNodeCount, "Should be three active nodes in the cluster.");
    }
  }
}
