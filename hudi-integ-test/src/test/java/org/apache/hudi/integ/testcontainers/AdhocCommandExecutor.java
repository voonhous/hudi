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