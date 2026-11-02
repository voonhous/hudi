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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Registry of schema mappers (RFC-88, type plane), discovered with {@link ServiceLoader} so that engine, format and
 * sync modules plug in without {@code hudi-common} depending on them.
 *
 * <p>Each module lists its implementations in {@code META-INF/services/<interface name>}. A key registered twice is
 * an error, so two modules cannot silently disagree on the mapping for the same boundary. An implementation whose
 * own dependencies are missing from the classpath is skipped with a warning.
 */
public final class SchemaMappers {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaMappers.class);

  private static final Map<Class<?>, Map<String, ?>> REGISTRY = new ConcurrentHashMap<>();

  private SchemaMappers() {
  }

  /**
   * The mapper for the given type system, e.g. {@link TypeSystems#SPARK}.
   *
   * @throws HoodieException if no module on the classpath registers one
   */
  @SuppressWarnings("unchecked")
  public static <T> TypeSystemMapper<T> typeSystem(String typeSystem) {
    return (TypeSystemMapper<T>) require(typeSystemMappers(), TypeSystemMapper.class, typeSystem);
  }

  public static Option<TypeSystemMapper<?>> findTypeSystem(String typeSystem) {
    return Option.ofNullable(typeSystemMappers().get(typeSystem));
  }

  /**
   * Keys of every registered {@link TypeSystemMapper}, sorted.
   */
  public static Set<String> typeSystems() {
    return typeSystemMappers().keySet();
  }

  /**
   * The mapper for the given file format, e.g. {@link FileFormats#PARQUET}.
   *
   * @throws HoodieException if no module on the classpath registers one
   */
  @SuppressWarnings("unchecked")
  public static <P> FormatSchemaMapper<P> format(String format) {
    return (FormatSchemaMapper<P>) require(formatMappers(), FormatSchemaMapper.class, format);
  }

  public static <P> FormatSchemaMapper<P> format(HoodieFileFormat format) {
    return format(FileFormats.key(format));
  }

  public static Option<FormatSchemaMapper<?>> findFormat(String format) {
    return Option.ofNullable(formatMappers().get(format));
  }

  /**
   * Keys of every registered {@link FormatSchemaMapper}, sorted.
   */
  public static Set<String> formats() {
    return formatMappers().keySet();
  }

  /**
   * Loads and caches every implementation of a mapper service, keyed by {@code keyFn}. Other modules use this for
   * their own mapper interfaces, such as catalog schema renderers in {@code hudi-sync-common}.
   *
   * @return an unmodifiable map sorted by key
   * @throws IllegalStateException if two implementations share a key
   */
  @SuppressWarnings("unchecked")
  public static <S> Map<String, S> load(Class<S> service, Function<S, String> keyFn) {
    return (Map<String, S>) REGISTRY.computeIfAbsent(service, s -> discover(service, keyFn));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, TypeSystemMapper<?>> typeSystemMappers() {
    return (Map) load(TypeSystemMapper.class, TypeSystemMapper::typeSystem);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, FormatSchemaMapper<?>> formatMappers() {
    return (Map) load(FormatSchemaMapper.class, FormatSchemaMapper::format);
  }

  private static <S> S require(Map<String, ? extends S> mappers, Class<?> service, String key) {
    S mapper = mappers.get(key);
    if (mapper == null) {
      throw new HoodieException("No " + service.getSimpleName() + " registered for '" + key + "'. Registered: "
          + mappers.keySet() + ". Is the module that provides it on the classpath?");
    }
    return mapper;
  }

  private static <S> Map<String, S> discover(Class<S> service, Function<S, String> keyFn) {
    Map<String, S> found = new TreeMap<>();
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    addAll(found, service, keyFn, contextLoader);
    ClassLoader ownLoader = SchemaMappers.class.getClassLoader();
    if (ownLoader != contextLoader) {
      addAll(found, service, keyFn, ownLoader);
    }
    return Collections.unmodifiableMap(found);
  }

  private static <S> void addAll(Map<String, S> found, Class<S> service, Function<S, String> keyFn, ClassLoader loader) {
    Iterator<S> it = ServiceLoader.load(service, loader).iterator();
    while (true) {
      S mapper;
      try {
        if (!it.hasNext()) {
          return;
        }
        mapper = it.next();
      } catch (ServiceConfigurationError | LinkageError e) {
        // A module can be on the classpath without the libraries its mapper needs (e.g. hudi-hadoop-mr without
        // hive-serde). Skip that mapper rather than losing every other one; looking it up then fails with
        // "No ... registered".
        LOG.warn("Skipping a {} that cannot be loaded: {}", service.getSimpleName(), e.toString());
        continue;
      }
      String key = keyFn.apply(mapper);
      S existing = found.get(key);
      if (existing == null) {
        found.put(key, mapper);
      } else if (existing.getClass() != mapper.getClass()) {
        throw new IllegalStateException(service.getSimpleName() + " '" + key + "' is registered by both "
            + existing.getClass().getName() + " and " + mapper.getClass().getName());
      }
      // The same class seen through both class loaders is the same registration.
    }
  }
}
