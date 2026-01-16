package org.apache.hudi.integ2.testcontainers.command;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ2.testcontainers.ContainerProvider;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.Arrays;
import java.util.List;

/**
 * Executor for Trino-specific commands in testcontainers environment.
 * Handles all Trino CLI operations.
 */
public class TrinoCommandExecutor extends CommandExecutor {

  private static final String ADHOC_1_CONTAINER = "adhoc-1";
  private static final String TRINO_COORDINATOR_URL = "trino-coordinator-1:8091";

  // Trino-specific demo file paths
  private static final String TRINO_TABLE_CHECK_FILENAME = "trino-table-check.commands";
  private static final String TRINO_BATCH1_FILENAME = "trino-batch1.commands";
  private static final String TRINO_BATCH2_FILENAME = "trino-batch2-after-compaction.commands";
  private static final String TRINO_INPUT_TABLE_CHECK_RELATIVE_PATH = "/docker/demo/" + TRINO_TABLE_CHECK_FILENAME;
  private static final String TRINO_INPUT_BATCH1_RELATIVE_PATH = "/docker/demo/" + TRINO_BATCH1_FILENAME;
  private static final String TRINO_INPUT_BATCH2_RELATIVE_PATH = "/docker/demo/" + TRINO_BATCH2_FILENAME;

  public TrinoCommandExecutor(ContainerProvider containerProvider) {
    super(containerProvider);
  }

  @Override
  protected ContainerState getContainer() {
    return containerProvider.getContainer(ADHOC_1_CONTAINER);
  }

  /**
   * Execute a Trino command file.
   */
  public Pair<String, String> executeFile(String commandFile) throws Exception {
    String trinoCmd = new StringBuilder()
        .append("trino --server ").append(TRINO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f ").append(commandFile)
        .toString();

    Container.ExecResult result = executeCommandString(trinoCmd, true);
    return Pair.of(result.getStdout().trim(), result.getStderr().trim());
  }

  /**
   * Setup demo by copying command files to the Trino execution container.
   *
   * @param workspaceRoot The workspace root directory (e.g., System.getProperty("user.dir") + "/..")
   * @param targetDir The target directory in the container where files will be copied
   */
  public void setupDemo(String workspaceRoot, String targetDir) {
    // Copy Trino-specific command files
    List<String> filesToCopy = Arrays.asList(
        workspaceRoot + TRINO_INPUT_TABLE_CHECK_RELATIVE_PATH,
        workspaceRoot + TRINO_INPUT_BATCH1_RELATIVE_PATH,
        workspaceRoot + TRINO_INPUT_BATCH2_RELATIVE_PATH
    );

    for (String file : filesToCopy) {
      copyFileToContainer(file, targetDir);
    }
  }
}