package org.apache.hudi.integ;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

@Testcontainers
public class TestDockerCompose {

  // Define service names and ports as constants for clarity
  private static final String NAMENODE_SERVICE = "namenode";

  // The @Container annotation marks this field as a managed container
  @Container
  public static ComposeContainer compose =
      new ComposeContainer(createProcessedComposeFile("/../docker/compose/docker-compose_hadoop284_hive233_spark353_arm64.yml"))
          .withEnv("HUDI_WS", new File(System.getProperty("user.dir"), "..").getAbsolutePath())
          // 1. Expose the main UI port and use its healthcheck as the primary wait condition.
          // This ensures the entire service is initialized before tests run.
          .withExposedService(NAMENODE_SERVICE, 50070,
              Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(3)));

  @Test
  void testWebServerIsRunning() {
    System.out.println(compose.getContainerByServiceName(NAMENODE_SERVICE).get().getContainerId());
  }


  /**
   * Reads the original docker-compose file, removes all 'container_name' directives, and returns a temporary file containing the modified content. Including Testcontainers in the docker-compose file
   * will cause ContainerLaunchExceptions to be thrown.
   * <p>
   * Any env_file will normalize/canonicalize to the temporary directory used. This function will make a copy of hadoop.env into the temporary directory to ensure that no error is thrown.
   */
  private static File createProcessedComposeFile(String composeFile) {
    try {
      // Normalize to canonicalize (remove ..)
      Path originalComposePath = Paths.get(System.getProperty("user.dir") + composeFile);

      // Read all bytes from the file and convert to a String
      byte[] bytes = Files.readAllBytes(originalComposePath);
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
      Path originalHadoopEnvPath = Paths.get(System.getProperty("user.dir"), "/../docker/compose/hadoop.env")
          .toAbsolutePath().normalize();
      Files.copy(originalHadoopEnvPath, destHadoopEnvPath, StandardCopyOption.REPLACE_EXISTING);

      // Return the temporary file for Testcontainers to use
      return tempDockerComposeFile;
    } catch (IOException e) {
      // If we can't process the file, we can't run the tests.
      throw new RuntimeException("Failed to process the docker-compose file", e);
    }
  }

}
