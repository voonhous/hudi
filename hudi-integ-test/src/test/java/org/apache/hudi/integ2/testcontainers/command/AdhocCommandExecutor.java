package org.apache.hudi.integ2.testcontainers.command;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ2.testcontainers.ContainerProvider;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.List;

/**
 * Executor for general-purpose commands in any container.
 * Handles HDFS commands, shell scripts, and other ad-hoc operations.
 */
public class AdhocCommandExecutor extends CommandExecutor {

  private final String containerName;

  public AdhocCommandExecutor(ContainerProvider containerProvider, String containerName) {
    super(containerProvider);
    this.containerName = containerName;
  }

  @Override
  protected ContainerState getContainer() {
    return containerProvider.getContainer(containerName);
  }

  /**
   * Execute a command string and return stdout/stderr.
   */
  public Pair<String, String> execute(String cmd) throws Exception {
    Container.ExecResult result = executeCommandString(cmd, true);
    return Pair.of(result.getStdout(), result.getStderr());
  }

  /**
   * Execute a command string with specified success expectation.
   */
  public Pair<String, String> execute(String cmd, boolean expectedToSucceed) throws Exception {
    Container.ExecResult result = executeCommandString(cmd, expectedToSucceed);
    return Pair.of(result.getStdout(), result.getStderr());
  }

  /**
   * Execute multiple command strings sequentially.
   */
  public void executeCommands(List<String> commands) throws Exception {
    for (String cmd : commands) {
      execute(cmd);
    }
  }
}
