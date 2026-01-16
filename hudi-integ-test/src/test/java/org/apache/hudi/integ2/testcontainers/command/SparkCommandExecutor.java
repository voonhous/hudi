package org.apache.hudi.integ2.testcontainers.command;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ2.testcontainers.ContainerProvider;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

/**
 * Executor for Spark-specific commands in testcontainers environment.
 * Handles all Spark shell and SQL operations.
 */
public class SparkCommandExecutor extends CommandExecutor {

  private static final String HADOOP_CONF_DIR = "/etc/hadoop";
  private static final String HUDI_SPARK_BUNDLE =
      "/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";

  // Container names
  private static final String ADHOC_1_CONTAINER = "adhoc1";

  public SparkCommandExecutor(ContainerProvider containerProvider) {
    super(containerProvider);
  }

  @Override
  protected ContainerState getContainer() {
    return containerProvider.getContainer(ADHOC_1_CONTAINER);
  }

  /**
   * Execute a Spark SQL command file.
   */
  public Pair<String, String> executeSQLFile(String commandFile, boolean expectedToSucceed) throws Exception {
    String sparkShellCmd = new StringBuilder()
        .append("spark-shell --jars ").append(HUDI_SPARK_BUNDLE)
        .append(" --master local[2] --driver-class-path ").append(HADOOP_CONF_DIR)
        .append(" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer")
        .append(" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .append(" --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .append(" --deploy-mode client --driver-memory 1G --executor-memory 1G --num-executors 1")
        .append(" -i ").append(commandFile)
        .toString();

    Container.ExecResult result = executeCommandString(sparkShellCmd, expectedToSucceed);
    return Pair.of(result.getStdout(), result.getStderr());
  }
}