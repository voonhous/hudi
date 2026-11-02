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

package org.apache.hudi.common.expression.translate;

import org.apache.hudi.common.schema.mapping.SchemaMappers;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.util.Map;
import java.util.Set;

/**
 * Registry of {@link ExpressionTranslator}s and {@link ExpressionRenderer}s, discovered with
 * {@link java.util.ServiceLoader} like the schema mappers.
 */
public final class ExpressionMappers {

  /**
   * Hive Metastore partition filter (also used for ADB and the Flink {@code HoodieHiveCatalog}).
   */
  public static final String HMS = "hms";
  public static final String GLUE = "glue";
  public static final String PARQUET = "parquet";
  /**
   * Spark data source {@code Filter}, which feeds Spark's vectorized Parquet reader.
   */
  public static final String SPARK_FILTER = "spark_filter";

  private ExpressionMappers() {
  }

  @SuppressWarnings("unchecked")
  public static <E> ExpressionTranslator<E> translator(String system) {
    ExpressionTranslator<?> translator = translators().get(system);
    if (translator == null) {
      throw new HoodieException("No ExpressionTranslator registered for '" + system + "'. Registered: " + translators().keySet());
    }
    return (ExpressionTranslator<E>) translator;
  }

  public static Option<ExpressionTranslator<?>> findTranslator(String system) {
    return Option.ofNullable(translators().get(system));
  }

  public static Set<String> translatorSystems() {
    return translators().keySet();
  }

  @SuppressWarnings("unchecked")
  public static <T> ExpressionRenderer<T> renderer(String target) {
    ExpressionRenderer<?> renderer = renderers().get(target);
    if (renderer == null) {
      throw new HoodieException("No ExpressionRenderer registered for '" + target + "'. Registered: " + renderers().keySet());
    }
    return (ExpressionRenderer<T>) renderer;
  }

  public static Option<ExpressionRenderer<?>> findRenderer(String target) {
    return Option.ofNullable(renderers().get(target));
  }

  public static Set<String> rendererTargets() {
    return renderers().keySet();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, ExpressionTranslator<?>> translators() {
    return (Map) SchemaMappers.load(ExpressionTranslator.class, ExpressionTranslator::system);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, ExpressionRenderer<?>> renderers() {
    return (Map) SchemaMappers.load(ExpressionRenderer.class, ExpressionRenderer::target);
  }
}
