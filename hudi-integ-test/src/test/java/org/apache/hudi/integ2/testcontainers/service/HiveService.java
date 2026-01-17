package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

/**
 * A service wrapper for the Hive container.
 * This class is responsible for all interactions with the Hive service,
 * including executing commands and managing files.
 */
public class HiveService {

  private static final String HIVESERVER_CONTAINER = "hiveserver";
  private static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  private static final String HUDI_HADOOP_BUNDLE =
      "/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";

  private final CommandExecutor executor;

  public HiveService(ContainerProvider provider) {
    this.executor = new CommandExecutor(provider.getContainer(HIVESERVER_CONTAINER));
  }

  /**
   * Execute a Hive command and return the result.
   */
  public CommandResult execute(String hiveCommand) throws Exception {
    String[] hiveCmd = {
        "hive",
        "--hiveconf", "hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat",
        "--hiveconf", "hive.stats.autogather=false",
        "-e", hiveCommand
    };
    return executor.executeCommand(hiveCmd);
  }

  /**
   * A higher-level method to execute a single Hive query.
   *
   * @param query The SQL query string to execute.
   * @return The result of the command execution.
   */
  public CommandResult runQuery(String query) throws Exception {
    return execute(String.format("\"%s\"", query));
  }

  /**
   * Execute a Hive command file and return the result.
   */
  public CommandResult executeFile(String commandFile) throws Exception {
    return executeFile(commandFile, null);
  }

  /**
   * Execute a Hive command file with additional variables.
   */
  public CommandResult executeFile(String commandFile, String additionalVar) throws Exception {
    StringBuilder hiveCmd = new StringBuilder()
        .append("beeline -u ").append(HIVE_SERVER_JDBC_URL)
        .append(" --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat")
        .append(" --hiveconf hive.stats.autogather=false")
        .append(" --hivevar hudi.hadoop.bundle=").append(HUDI_HADOOP_BUNDLE);

    if (additionalVar != null) {
      hiveCmd.append(" --hivevar ").append(additionalVar);
    }
    hiveCmd.append(" -f ").append(commandFile);

    return executor.executeCommandString(hiveCmd.toString());
  }

  /**
   * Copy a file from the host to the Hive container.
   */
  public void copyFile(String fromFile, String remotePath) {
    executor.copyFileToContainer(fromFile, remotePath);
  }
}
