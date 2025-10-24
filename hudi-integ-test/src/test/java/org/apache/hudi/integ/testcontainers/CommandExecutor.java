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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Base class for command executors that run commands in specific containers.
 * Each executor handles operations for a specific component (Hive, Spark, etc.).
 */
public abstract class CommandExecutor {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final ContainerProvider containerProvider;

  protected CommandExecutor(ContainerProvider containerProvider) {
    this.containerProvider = containerProvider;
  }

  /**
   * Get the container for this executor.
   */
  protected abstract ContainerState getContainer();

  /**
   * Get a readable identifier for the container.
   * Subclasses can override to provide more meaningful names.
   */
  protected String getContainerIdentifier(ContainerState container) {
    String containerName = container.getContainerInfo().getName();
    if (containerName != null && !containerName.isEmpty()) {
      // Container names start with '/', so remove it
      String cleanName = containerName.startsWith("/") ? containerName.substring(1) : containerName;
      return cleanName + ":" + container.getContainerId().substring(0, 8);
    }
    return container.getContainerId().substring(0, 12);
  }

  /**
   * Execute a command in the executor's container.
   */
  protected Container.ExecResult executeCommand(String... command) throws Exception {
    return executeCommand(true, true, command);
  }

  /**
   * Execute a command with options to check success.
   */
  protected Container.ExecResult executeCommand(
      boolean checkIfSucceed,
      boolean expectedToSucceed,
      String... command) throws Exception {

    ContainerState container = getContainer();
    String containerIdentifier = getContainerIdentifier(container);
    String commandStr = String.join(" ", command);

    log.info("==> [{}] Executing: {}", containerIdentifier, commandStr);

    long startTime = System.currentTimeMillis();
    Container.ExecResult result = container.execInContainer(command);
    long duration = System.currentTimeMillis() - startTime;

    int exitCode = result.getExitCode();
    log.info("<== [{}] Exit code: {} ({}ms)", containerIdentifier, exitCode, duration);

    if (exitCode != 0) {
      log.error("STDOUT:\n{}", result.getStdout());
      log.error("STDERR:\n{}", result.getStderr());
    } else if (log.isDebugEnabled()) {
      log.debug("STDOUT:\n{}", result.getStdout());
      if (!result.getStderr().isEmpty()) {
        log.debug("STDERR:\n{}", result.getStderr());
      }
    }

    if (checkIfSucceed) {
      if (expectedToSucceed) {
        assertEquals(0, exitCode, "Command (" + commandStr + ") expected to succeed. Exit (" + exitCode + ")");
      } else {
        assertNotEquals(0, exitCode, "Command (" + commandStr + ") expected to fail. Exit (" + exitCode + ")");
      }
    }

    return result;
  }

  /**
   * Execute a shell command string.
   */
  protected Container.ExecResult executeCommandString(String cmd, boolean expectedToSucceed) throws Exception {
    return executeCommandString(cmd, true, expectedToSucceed);
  }

  /**
   * Execute a shell command string with option to check success.
   */
  protected Container.ExecResult executeCommandString(
      String cmd, boolean checkIfSucceed, boolean expectedToSucceed) throws Exception {
    String[] cmdArray = {"/bin/bash", "-c", cmd};
    return executeCommand(checkIfSucceed, expectedToSucceed, cmdArray);
  }

  /**
   * Copy a file from the host to the container.
   */
  protected void copyFileToContainer(String fromFile, String remotePath) {
    try {
      ContainerState container = getContainer();
      MountableFile mountableFile = MountableFile.forHostPath(Paths.get(fromFile));
      container.copyFileToContainer(mountableFile, remotePath);
      log.info("Successfully copied file {} to container at path {}", fromFile, remotePath);
    } catch (Exception e) {
      log.error("Failed to copy file {} to container at path {}", fromFile, remotePath, e);
      throw new RuntimeException("Failed to copy file to container", e);
    }
  }
}
