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

package org.apache.hudi.integ.testcontainers;

import org.apache.hudi.common.util.collection.Pair;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import static org.apache.hudi.integ.ITTestBaseTestcontainers.ADHOC_1_CONTAINER;

/**
 * Executor for Spark-specific commands in testcontainers environment.
 * Handles all Spark shell and SQL operations.
 */
public class SparkCommandExecutor extends CommandExecutor {

  private static final String HADOOP_CONF_DIR = "/etc/hadoop";
  private static final String HUDI_SPARK_BUNDLE =
      "/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";

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