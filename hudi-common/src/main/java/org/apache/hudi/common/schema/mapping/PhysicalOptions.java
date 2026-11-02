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

package org.apache.hudi.common.schema.mapping;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Options for a {@link FormatSchemaMapper}: the physical layout knobs that exist today (legacy list and decimal
 * layouts, timestamp NTZ, variant shredding, blob encoding). Every knob keeps today's default when absent.
 */
public final class PhysicalOptions extends AbstractMapperOptions {

  private static final long serialVersionUID = 1L;

  /**
   * No options: every mapper uses today's defaults.
   */
  public static final PhysicalOptions DEFAULT = new PhysicalOptions(Collections.emptyMap());

  private PhysicalOptions(Map<String, String> options) {
    super(options);
  }

  public static PhysicalOptions of(Map<String, String> options) {
    return options.isEmpty() ? DEFAULT : new PhysicalOptions(options);
  }

  public static PhysicalOptions fromProperties(Properties props) {
    return of(toMap(props));
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link PhysicalOptions}.
   */
  public static final class Builder {
    private final Map<String, String> options = new TreeMap<>();

    private Builder() {
    }

    public Builder set(String key, String value) {
      options.put(key, value);
      return this;
    }

    public Builder set(String key, boolean value) {
      return set(key, String.valueOf(value));
    }

    public Builder setAll(Map<String, String> values) {
      options.putAll(values);
      return this;
    }

    public PhysicalOptions build() {
      return of(options);
    }
  }
}
