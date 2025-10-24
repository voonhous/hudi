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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testcontainers.AdhocCommandExecutor;
import org.apache.hudi.integ.testcontainers.ContainerProvider;
import org.apache.hudi.integ.testcontainers.HiveCommandExecutor;
import org.apache.hudi.integ.testcontainers.PrestoCommandExecutor;
import org.apache.hudi.integ.testcontainers.SparkCommandExecutor;
import org.apache.hudi.integ.testcontainers.TrinoCommandExecutor;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base test class for integration tests using Testcontainers with Docker Compose.
 * Uses composition pattern with specialized executors for different components.
 * This provides better separation of concerns and easier maintenance.
 */
@Testcontainers
public abstract class ITTestBaseTestcontainers implements ContainerProvider {

  public static final Logger LOG = LoggerFactory.getLogger(ITTestBaseTestcontainers.class);

  protected static final String HIVESERVER_CONTAINER = "hiveserver";
  protected static final String NAMENODE_CONTAINER = "namenode";
  protected static final String DATANODE_1_CONTAINER = "datanode1";
  protected static final String HISTORY_SERVER_CONTAINER = "historyserver";
  protected static final String HIVE_METASTORE_PGSQL_CONTAINER = "hive-metastore-postgresql";
  protected static final String HIVE_METASTORE_CONTAINER = "hivemetastore";
  protected static final String ZOOKEEPER_CONTAINER = "zookeeper";
  protected static final String KAFKA_CONTAINER = "kafka";
  protected static final String SPARK_MASTER_CONTAINER = "sparkmaster";
  protected static final String SPARK_WORKER_1_CONTAINER = "sparkmaster";
  public static final String ADHOC_1_CONTAINER = "adhoc1";
  protected static final String ADHOC_2_CONTAINER = "adhoc2";

  protected static final String PRESTO_COORDINATOR = "presto-coordinator-1";
  protected static final String TRINO_COORDINATOR = "trino-coordinator-1";
  protected static final String HOODIE_WS_ROOT = "/var/hoodie/ws";
  protected static final String HOODIE_JAVA_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_app.sh";
  protected static final String HOODIE_GENERATE_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_generate_app.sh";
  protected static final String HOODIE_JAVA_STREAMING_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_streaming_app.sh";
  protected static final String HUDI_HADOOP_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";
  protected static final String HUDI_HIVE_SYNC_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hive-sync-bundle.jar";
  protected static final String HUDI_SPARK_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";
  protected static final String HUDI_UTILITIES_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar";
  protected static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  protected static final String PRESTO_COORDINATOR_URL = "presto-coordinator-1:8090";
  protected static final String TRINO_COORDINATOR_URL = "trino-coordinator-1:8091";
  protected static final String HADOOP_CONF_DIR = "/etc/hadoop";

  protected static ComposeContainer environment;

  // Composed executors for different components
  protected HiveCommandExecutor hive;
  protected SparkCommandExecutor spark;
  protected PrestoCommandExecutor presto;
  protected TrinoCommandExecutor trino;
  protected AdhocCommandExecutor adhoc1;
  protected AdhocCommandExecutor adhoc2;

  @BeforeAll
  public static void setupDockerCompose() {
    String composeFilePath = createProcessedComposeFilePath(getDockerComposeFilePath());
    String hudiWorkspace = getHudiWorkspace();

    LOG.info("Starting Docker Compose environment");
    LOG.info("Compose file: {}", composeFilePath);
    LOG.info("HUDI_WS: {}", hudiWorkspace);


    environment = new ComposeContainer(new File(composeFilePath))
        .withEnv("HUDI_WS", hudiWorkspace)
        .withExposedService(SPARK_MASTER_CONTAINER, 8080, Wait.forListeningPort().forPorts(8080).withStartupTimeout(Duration.ofMinutes(1)))
        .withStartupTimeout(Duration.ofMinutes(2));
    environment.start();

    LOG.info("Docker Compose environment started successfully");
    LOG.info("All containers verified and running");
  }

  /**
   * Initialize executors. Should be called in @BeforeEach or constructor of test class.
   */
  protected void initializeExecutors() {
    this.hive = new HiveCommandExecutor(this);
    this.spark = new SparkCommandExecutor(this);
//    this.presto = new PrestoCommandExecutor(this);
//    this.trino = new TrinoCommandExecutor(this);
    this.adhoc1 = new AdhocCommandExecutor(this, ADHOC_1_CONTAINER);
    this.adhoc2 = new AdhocCommandExecutor(this, ADHOC_2_CONTAINER);
  }

  /**
   * Get a container by service name from the Docker Compose environment.
   * Docker Compose appends _1 suffix to service names.
   * Implements ContainerProvider interface.
   */
  @Override
  public ContainerState getContainer(String serviceName) {
    try {
      return environment.getContainerByServiceName(serviceName)
          .orElseThrow(() -> new IllegalStateException("Container not found: " + serviceName));
    } catch (IllegalStateException e) {
      LOG.error("Failed to get container: {}", serviceName, e);
      throw e;
    }
  }

  /**
   * Copy a file to a container.
   */
  protected void executeCopyCommand(String containerName, String fromFile, String remotePath) {
    try {
      ContainerState container = getContainer(containerName);
      MountableFile mountableFile = MountableFile.forHostPath(Paths.get(fromFile));
      container.copyFileToContainer(mountableFile, remotePath);
      LOG.info("Successfully copied file {} to container {} at path {}", fromFile, containerName, remotePath);
    } catch (Exception e) {
      LOG.error("Failed to copy file {} to container {} at path {}", fromFile, containerName, remotePath, e);
      throw new RuntimeException("Failed to copy file to container", e);
    }
  }

  /**
   * Assert that stdout contains expected output.
   */
  protected void assertStdOutContains(Pair<String, String> stdOutErr, String expectedOutput) {
    assertStdOutContains(stdOutErr, expectedOutput, 1);
  }

  /**
   * Assert that stdout contains expected output a specific number of times.
   */
  protected void assertStdOutContains(Pair<String, String> stdOutErr, String expectedOutput, int times) {
    // Remove extra spaces for comparison
    String stdOutSingleSpaced = stdOutErr.getLeft().replaceAll("[\\s]+", " ").replaceAll(" ", "");
    expectedOutput = expectedOutput.replaceAll("[\\s]+", " ").replaceAll(" ", "");

    int lastIndex = 0;
    int count = 0;
    while (lastIndex != -1) {
      lastIndex = stdOutSingleSpaced.indexOf(expectedOutput, lastIndex);
      if (lastIndex != -1) {
        count++;
        lastIndex += expectedOutput.length();
      }
    }

    assertEquals(times, count, "Did not find output the expected number of times.");
  }

  private static String getHudiWorkspace() {
    String projectDir = System.getProperty("user.dir");
    return new File(projectDir, "..").getAbsolutePath();
  }

  private static String getDockerComposeFilePath() {
    String projectDir = System.getProperty("user.dir");
    String os = System.getProperty("os.name").toLowerCase();
    String arch = System.getProperty("os.arch").toLowerCase();

    // Determine which compose file to use based on OS and architecture
    if (os.contains("mac") && arch.contains("aarch64")) {
      return new File(projectDir, "../docker/compose/docker-compose_hadoop284_hive233_spark353_arm64.yml").getAbsolutePath();
    } else {
      return new File(projectDir, "../docker/compose/docker-compose_hadoop284_hive233_spark353_amd64.yml").getAbsolutePath();
    }
  }

  private static String getHadoopEnvFilePath() {
    return new File(System.getProperty("user.dir"), "../docker/compose/hadoop.env").getAbsolutePath();
  }

  /**
   * Reads the original docker-compose file, removes all 'container_name' directives, and returns a temporary file containing the modified content. Including Testcontainers in the docker-compose file
   * will cause ContainerLaunchExceptions to be thrown.
   * <p>
   * Any env_file will normalize/canonicalize to the temporary directory used. This function will make a copy of hadoop.env into the temporary directory to ensure that no error is thrown.
   */
  private static String createProcessedComposeFilePath(String composeFile) {
    try {
      // Read all bytes from the file and convert to a String
      byte[] bytes = Files.readAllBytes(Paths.get(composeFile));
      String originalContent = new String(bytes, StandardCharsets.UTF_8);

      // Use a regular expression to find and remove all lines containing 'container_name'
      String modifiedContent = originalContent.replaceAll("(?m)^\\s*container_name:.*$", "");

      // Create a temporary file to hold our modified configuration
      Path tempDir = Files.createTempDirectory("hudi-test-compose-");

      // Ensure the temporary directory is deleted when the JVM exits
      tempDir.toFile().deleteOnExit();

      // Write the modified content to the temporary file as a byte array.
      File tempDockerComposeFile = new File(tempDir.toFile(), "docker-compose.yml");
      Files.write(tempDockerComposeFile.toPath(), modifiedContent.getBytes(StandardCharsets.UTF_8));

      // tempDir is used as the working directory, docker-compose will look for hadoop.env in the SAME temp directory
      // Copy hadoop.env into the SAME temp directory
      Path destHadoopEnvPath = tempDir.resolve("hadoop.env");
      Path originalHadoopEnvPath = Paths.get(getHadoopEnvFilePath()).toAbsolutePath().normalize();
      Files.copy(originalHadoopEnvPath, destHadoopEnvPath, StandardCopyOption.REPLACE_EXISTING);

      // Return the temporary file for Testcontainers to use
      return tempDockerComposeFile.toPath().toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to process the docker-compose file", e);
    }
  }
}