package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

/**
 * A service wrapper for the Presto container.
 * This class is responsible for all interactions with the Presto service.
 */
public class PrestoService {

  private static final String PRESTO_COORDINATOR = "presto-coordinator-1";
  private static final String PRESTO_COORDINATOR_URL = "presto-coordinator-1:8090";

  private final CommandExecutor executor;

  public PrestoService(ContainerProvider provider) {
    this.executor = new CommandExecutor(provider.getContainer(PRESTO_COORDINATOR));
  }

  /**
   * Execute a Presto command file.
   */
  public CommandResult executeFile(String commandFile) throws Exception {
    String prestoCmd = new StringBuilder()
        .append("presto --server ").append(PRESTO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f ").append(commandFile)
        .toString();

    return executor.executeCommandString(prestoCmd);
  }

  /**
   * Copy a file from the host to the Presto container.
   */
  public void copyFile(String fromFile, String remotePath) {
    executor.copyFileToContainer(fromFile, remotePath);
  }
}
