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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Executor for Presto-specific commands in testcontainers environment. Handles all Presto CLI operations.
 */
public class PrestoCommandExecutor extends CommandExecutor {

  private static final String PRESTO_COORDINATOR = "presto-coordinator-1";
  private static final String PRESTO_COORDINATOR_URL = "presto-coordinator-1:8090";

  // Presto-specific demo file paths
  private static final String PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH = "/docker/demo/presto-table-check.commands";
  private static final String PRESTO_INPUT_BATCH1_RELATIVE_PATH = "/docker/demo/presto-batch1.commands";
  private static final String PRESTO_INPUT_BATCH2_RELATIVE_PATH = "/docker/demo/presto-batch2-after-compaction.commands";

  public PrestoCommandExecutor(ContainerProvider containerProvider) {
    super(containerProvider);
  }

  @Override
  protected ContainerState getContainer() {
    return containerProvider.getContainer(PRESTO_COORDINATOR);
  }

  /**
   * Execute a Presto command file.
   */
  public Pair<String, String> executeFile(String commandFile) throws Exception {
    String prestoCmd = new StringBuilder()
        .append("presto --server ").append(PRESTO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f ").append(commandFile)
        .toString();

    Container.ExecResult result = executeCommandString(prestoCmd, true);
    return Pair.of(result.getStdout().trim(), result.getStderr().trim());
  }

  /**
   * Setup demo by creating directories and copying command files to Presto coordinator.
   *
   * @param workspaceRoot The workspace root directory (e.g., System.getProperty("user.dir") + "/..")
   * @param targetDir     The target directory in the container where files will be copied
   */
  public void setupDemo(String workspaceRoot, String targetDir) throws Exception {
    // Create input directory
    executeCommandString("mkdir -p " + targetDir, true);

    // Copy Presto-specific command files
    List<String> filesToCopy = Arrays.asList(
        workspaceRoot + PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH,
        workspaceRoot + PRESTO_INPUT_BATCH1_RELATIVE_PATH,
        workspaceRoot + PRESTO_INPUT_BATCH2_RELATIVE_PATH
    );

    for (String file : filesToCopy) {
      copyFileToContainer(file, targetDir);
    }
  }
}