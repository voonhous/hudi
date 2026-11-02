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

import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Immutable string key-value options passed to a mapper. Unknown keys are ignored, so each mapper reads only the
 * keys it documents and every key defaults to today's behaviour when absent.
 */
public abstract class AbstractMapperOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, String> options;

  protected AbstractMapperOptions(Map<String, String> options) {
    this.options = Collections.unmodifiableMap(new TreeMap<>(options));
  }

  protected static Map<String, String> toMap(Properties props) {
    Map<String, String> map = new TreeMap<>();
    props.stringPropertyNames().forEach(key -> map.put(key, props.getProperty(key)));
    return map;
  }

  public Option<String> get(String key) {
    return Option.ofNullable(options.get(key));
  }

  public String getString(String key, String defaultValue) {
    return options.getOrDefault(key, defaultValue);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String value = options.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value.trim());
  }

  public int getInt(String key, int defaultValue) {
    String value = options.get(key);
    return value == null ? defaultValue : Integer.parseInt(value.trim());
  }

  public boolean contains(String key) {
    return options.containsKey(key);
  }

  public Map<String, String> asMap() {
    return options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return options.equals(((AbstractMapperOptions) o).options);
  }

  @Override
  public int hashCode() {
    return options.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + options;
  }
}
