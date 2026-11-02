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

package org.apache.hudi.metadata.planner;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helpers shared by the lookups.
 */
final class LookupUtils {

  private LookupUtils() {
  }

  /**
   * The column names the filter references, in order of appearance.
   */
  static Set<String> referencedColumns(Expression filter) {
    Set<String> names = new LinkedHashSet<>();
    collectReferences(filter, names);
    return names;
  }

  private static void collectReferences(Expression expr, Set<String> names) {
    if (expr == null) {
      return;
    }
    if (expr instanceof NameReference) {
      names.add(((NameReference) expr).getName());
      return;
    }
    List<Expression> children = expr.getChildren();
    if (children != null) {
      for (Expression child : children) {
        collectReferences(child, names);
      }
    }
  }

  /**
   * The operands of the top-level ANDs of the filter.
   */
  static List<Expression> conjuncts(Expression filter) {
    List<Expression> conjuncts = new ArrayList<>();
    collectConjuncts(filter, conjuncts);
    return conjuncts;
  }

  private static void collectConjuncts(Expression expr, List<Expression> conjuncts) {
    if (expr instanceof Predicates.And) {
      collectConjuncts(((Predicates.And) expr).getLeft(), conjuncts);
      collectConjuncts(((Predicates.And) expr).getRight(), conjuncts);
    } else if (expr != null) {
      conjuncts.add(expr);
    }
  }

  /**
   * The columns whose column stats (and partition stats) can be used for pruning, as Spark's
   * {@code ColumnStatsIndexSupport} and {@code PartitionStatsIndexSupport} determine them: the source fields of the
   * column stats index definition, without the timestamp-millis columns of a V1 index
   * ({@link HoodieTableMetadataUtil#getValidIndexedColumns}). Partition stats share the column stats columns; their
   * own definition is used only when the column stats one is missing.
   *
   * @return empty if the table has no such index definition
   */
  static List<String> validIndexedColumns(HoodieTableMetaClient metaClient, HoodieSchema tableSchema, String metadataPartition) {
    Option<HoodieIndexDefinition> definition = metaClient.getIndexForMetadataPartition(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    if (!definition.isPresent()) {
      definition = metaClient.getIndexForMetadataPartition(metadataPartition);
    }
    if (!definition.isPresent() || definition.get().getSourceFields() == null) {
      return Collections.emptyList();
    }
    return HoodieTableMetadataUtil.getValidIndexedColumns(definition.get(), tableSchema, metaClient.getTableConfig());
  }

  /**
   * The referenced columns that are indexed, with their non-null types, in order of reference.
   */
  static Map<String, HoodieSchema> prunableColumns(Expression filter, IndexLookupContext context, String metadataPartition) {
    Set<String> referenced = referencedColumns(filter);
    if (referenced.isEmpty() || context.tableSchema() == null) {
      return Collections.emptyMap();
    }
    List<String> indexed = validIndexedColumns(context.metaClient(), context.tableSchema(), metadataPartition);
    Map<String, HoodieSchema> columns = new LinkedHashMap<>();
    for (String name : referenced) {
      if (indexed.contains(name)) {
        Option<HoodieSchema> type = columnType(context.tableSchema(), name);
        if (type.isPresent()) {
          columns.put(name, type.get());
        }
      }
    }
    return columns;
  }

  /**
   * The non-null type of a (possibly nested, dot-separated) column.
   */
  static Option<HoodieSchema> columnType(HoodieSchema tableSchema, String name) {
    try {
      Option<Pair<String, HoodieSchemaField>> field = HoodieSchemaUtils.getNestedField(tableSchema, name);
      return field.map(f -> f.getRight().getNonNullSchema());
    } catch (RuntimeException e) {
      return Option.empty();
    }
  }

  /**
   * Every candidate file name of the context.
   */
  static Set<String> allFiles(IndexLookupContext context) {
    Set<String> files = new LinkedHashSet<>();
    context.filesByPartition().values().forEach(files::addAll);
    return files;
  }

  /**
   * The context restricted to the candidate files. Partitions that lose all their files are dropped, as partition
   * pruning drops them; partitions that had no files stay.
   */
  static IndexLookupContext narrow(IndexLookupContext context, CandidateFiles candidates) {
    if (candidates.isAll()) {
      return context;
    }
    Map<String, List<String>> narrowed = new LinkedHashMap<>();
    for (Map.Entry<String, List<String>> partition : context.filesByPartition().entrySet()) {
      List<String> kept = new ArrayList<>();
      for (String file : partition.getValue()) {
        if (candidates.fileNames().contains(file)) {
          kept.add(file);
        }
      }
      if (!kept.isEmpty() || partition.getValue().isEmpty()) {
        narrowed.put(partition.getKey(), kept);
      }
    }
    return new IndexLookupContext(context.metaClient(), context.metadata(), context.engineContext(), context.tableSchema(),
        context.evaluator(), narrowed);
  }
}
