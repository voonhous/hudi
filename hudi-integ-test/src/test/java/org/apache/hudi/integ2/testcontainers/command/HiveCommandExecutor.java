package org.apache.hudi.integ2.testcontainers.command;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ2.testcontainers.ContainerProvider;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

/**
 * Executor for Hive-specific commands in testcontainers environment.
 * Handles all Hive and Beeline operations.
 */
public class HiveCommandExecutor extends CommandExecutor {

  private static final String HIVESERVER_CONTAINER = "hiveserver";
  private static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  private static final String HUDI_HADOOP_BUNDLE =
      "/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";

  public HiveCommandExecutor(ContainerProvider containerProvider) {
    super(containerProvider);
  }

  @Override
  protected ContainerState getContainer() {
    return containerProvider.getContainer(HIVESERVER_CONTAINER);
  }

  @Override
  protected String getContainerIdentifier(ContainerState container) {
    return HIVESERVER_CONTAINER + ":" + container.getContainerId().substring(0, 8);
  }

  /**
   * Execute a Hive command and return stdout and stderr.
   */
  public Pair<String, String> execute(String hiveCommand) throws Exception {
    String[] hiveCmd = {
        "hive",
        "--hiveconf", "hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat",
        "--hiveconf", "hive.stats.autogather=false",
        "-e", hiveCommand
    };

    Container.ExecResult result = executeCommand(hiveCmd);
    return Pair.of(result.getStdout().trim(), result.getStderr().trim());
  }

  /**
   * Execute a Hive command file and return stdout and stderr.
   */
  public Pair<String, String> executeFile(String commandFile) throws Exception {
    return executeFile(commandFile, null);
  }

  /**
   * Execute a Hive command file with additional variables.
   */
  public Pair<String, String> executeFile(String commandFile, String additionalVar) throws Exception {
    StringBuilder hiveCmd = new StringBuilder()
        .append("beeline -u ").append(HIVE_SERVER_JDBC_URL)
        .append(" --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat")
        .append(" --hiveconf hive.stats.autogather=false")
        .append(" --hivevar hudi.hadoop.bundle=").append(HUDI_HADOOP_BUNDLE);

    if (additionalVar != null) {
      hiveCmd.append(" --hivevar ").append(additionalVar);
    }
    hiveCmd.append(" -f ").append(commandFile);

    Container.ExecResult result = executeCommandString(hiveCmd.toString(), true);
    return Pair.of(result.getStdout().trim(), result.getStderr().trim());
  }
}