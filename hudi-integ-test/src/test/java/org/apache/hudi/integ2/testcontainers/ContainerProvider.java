package org.apache.hudi.integ2.testcontainers;

import org.testcontainers.containers.ContainerState;

/**
 * Interface for providing access to containers by service name.
 * This abstraction allows different implementations (e.g., ComposeContainer, individual containers).
 */
public interface ContainerProvider {

  /**
   * Get a container by its service name.
   *
   * @param serviceName the name of the service (without _1 suffix)
   * @return the container state
   * @throws IllegalStateException if container not found
   */
  ContainerState getContainer(String serviceName);
}
