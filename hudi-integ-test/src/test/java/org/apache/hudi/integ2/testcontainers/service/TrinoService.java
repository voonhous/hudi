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

package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

/**
 * A service wrapper for the Trino container.
 * This class is responsible for all interactions with the Trino service.
 */
public class TrinoService {

  private static final String ADHOC_1_CONTAINER = "adhoc1";
  private static final String TRINO_COORDINATOR_URL = "trino-coordinator-1:8091";

  private final CommandExecutor executor;

  public TrinoService(ContainerProvider provider) {
    this.executor = new CommandExecutor(provider.getContainer(ADHOC_1_CONTAINER));
  }

  /**
   * Execute a Trino command file.
   */
  public CommandResult executeFile(String commandFile) throws Exception {
    String trinoCmd = new StringBuilder()
        .append("trino --server ").append(TRINO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f ").append(commandFile)
        .toString();

    return executor.executeCommandString(trinoCmd);
  }

  /**
   * Copy a file from the host to the Trino execution container.
   */
  public void copyFile(String fromFile, String remotePath) {
    executor.copyFileToContainer(fromFile, remotePath);
  }
}
