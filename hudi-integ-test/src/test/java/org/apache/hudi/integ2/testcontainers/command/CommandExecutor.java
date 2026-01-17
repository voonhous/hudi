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

package org.apache.hudi.integ2.testcontainers.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Paths;

/**
 * A utility class for executing commands within a given Testcontainer.
 * This class is stateless regarding the command being executed but is tied to a specific container.
 */
public final class CommandExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  private final ContainerState container;

  public CommandExecutor(ContainerState container) {
    this.container = container;
  }

  /**
   * Execute a command in the executor's container.
   */
  public CommandResult executeCommand(String... command) throws Exception {
    String containerIdentifier = getContainerIdentifier(container);
    String commandStr = String.join(" ", command);

    LOG.info("==> [{}] Executing: {}", containerIdentifier, commandStr);

    long startTime = System.currentTimeMillis();
    Container.ExecResult result = container.execInContainer(command);
    long duration = System.currentTimeMillis() - startTime;

    int exitCode = result.getExitCode();
    LOG.info("<== [{}] Exit code: {} ({}ms)", containerIdentifier, exitCode, duration);

    if (exitCode != 0) {
      LOG.error("STDOUT:\n{}", result.getStdout());
      LOG.error("STDERR:\n{}", result.getStderr());
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("STDOUT:\n{}", result.getStdout());
      if (!result.getStderr().isEmpty()) {
        LOG.debug("STDERR:\n{}", result.getStderr());
      }
    }

    return new CommandResult(result);
  }

  /**
   * Execute a shell command string.
   */
  public CommandResult executeCommandString(String cmd) throws Exception {
    String[] cmdArray = {"/bin/bash", "-c", cmd};
    return executeCommand(cmdArray);
  }

  /**
   * Copy a file from the host to the container.
   */
  public void copyFileToContainer(String fromFile, String remotePath) {
    try {
      MountableFile mountableFile = MountableFile.forHostPath(Paths.get(fromFile));
      container.copyFileToContainer(mountableFile, remotePath);
      LOG.info("Successfully copied file {} to container at path {}", fromFile, remotePath);
    } catch (Exception e) {
      LOG.error("Failed to copy file {} to container at path {}", fromFile, remotePath, e);
      throw new RuntimeException("Failed to copy file to container", e);
    }
  }

  /**
   * Get a readable identifier for the container.
   */
  private String getContainerIdentifier(ContainerState container) {
    String containerName = container.getContainerInfo().getName();
    if (containerName != null && !containerName.isEmpty()) {
      // Container names start with '/', so remove it
      String cleanName = containerName.startsWith("/") ? containerName.substring(1) : containerName;
      return cleanName + ":" + container.getContainerId().substring(0, 8);
    }
    return container.getContainerId().substring(0, 12);
  }
}
