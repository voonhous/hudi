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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.fs.FileNameParser;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.index.record.HoodieRecordIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Prunes files with the RECORD_INDEX: when the filter's top-level conjuncts include {@code key = v} or
 * {@code key IN (v1, ...)} on the record key, the record index gives the file groups holding those keys, and only
 * their files are kept. Mirrors Spark's {@code RecordLevelIndexSupport}, {@code GlobalRecordLevelIndexSupport} and
 * {@code PartitionedRecordLevelIndexSupport}:
 * <ul>
 *   <li>the record key fields come from the table config; a conjunct on a record key field or on
 *       {@code _hoodie_record_key} gives literals; the literals of every conjunct on the same field are united;</li>
 *   <li>a key with several fields, or a single-field complex key whose encoding prefixes field names, is looked up
 *       as {@code field1:v1,field2:v2} for every combination of the fields' literals, and needs literals for every
 *       field; a single-field complex key whose encoding is unknown is not looked up;</li>
 *   <li>a global index is probed once; a partitioned one per candidate partition (each probe within one shard), and
 *       not at all when there are more than {@value #MAX_PARTITIONS} candidate partitions or none of them is
 *       indexed;</li>
 *   <li>the files kept are those whose file id the index returned (files of the same file slice included).</li>
 * </ul>
 *
 * <p>Differences from Spark:
 * <ul>
 *   <li>the conjuncts are those of the whole filter, flattened, so {@code key = 'a' AND x > 1} uses the index in a
 *       single filter expression as it does when Spark passes the two as separate filters;</li>
 *   <li>only STRING, INT and LONG literals are used, whose string form is the stored key; Spark uses the literal's
 *       {@code toString}, which for dates, timestamps and decimals is not the key and so prunes every file. An empty
 *       string literal, stored as {@code __empty__} in complex keys, is not used either;</li>
 *   <li>for a complex key, literals on {@code _hoodie_record_key} are whole keys and are looked up as they are (Spark
 *       prefixes them with the field name {@code _hoodie_record_key});</li>
 *   <li>with a partitioned index, the files of candidate partitions the index has no file group for are kept (Spark
 *       prunes them), and a file name that does not parse as a Hudi file name is kept;</li>
 *   <li>the encoding of a single-field complex key is read from the table config (see
 *       {@link #encodingFromTableConfig}); Spark also deduces it from the data. Engines can pass their own resolver,
 *       e.g. {@code KeyGenUtils::resolveComplexKeyGenEncoding};</li>
 *   <li>the query type is not checked: like Spark, callers must not use the record index for incremental, time travel
 *       (before the latest commit) or read-optimized queries.</li>
 * </ul>
 */
public class RecordIndexLookup implements IndexLookup {

  /**
   * The most candidate partitions a partitioned record index is probed for, as in Spark's
   * {@code RecordLevelIndexSupport.MAX_PARTITIONS}.
   */
  public static final int MAX_PARTITIONS = 10;

  /**
   * The most keys looked up for one filter; more combinations of composite key literals skip the index.
   */
  static final int MAX_KEYS = 100_000;

  private final Function<HoodieTableMetaClient, Option<ComplexKeyGenEncoding>> encodingResolver;

  public RecordIndexLookup() {
    this(RecordIndexLookup::encodingFromTableConfig);
  }

  /**
   * @param encodingResolver resolves the record key encoding of a single-field complex key generator table; empty
   *                         means unknown, and the index is then not used
   */
  public RecordIndexLookup(Function<HoodieTableMetaClient, Option<ComplexKeyGenEncoding>> encodingResolver) {
    this.encodingResolver = encodingResolver;
  }

  @Override
  public MetadataPartitionType index() {
    return MetadataPartitionType.RECORD_INDEX;
  }

  @Override
  public boolean canPrune(Expression filter, IndexLookupContext context) {
    return filter != null
        && context.metaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)
        && !recordKeys(filter, context.metaClient()).isEmpty();
  }

  @Override
  public CandidateFiles lookup(Expression filter, IndexLookupContext context) {
    if (!context.metaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
      return CandidateFiles.all();
    }
    List<String> keys = recordKeys(filter, context.metaClient());
    if (keys.isEmpty()) {
      return CandidateFiles.all();
    }
    boolean partitioned = context.metaClient().getIndexForMetadataPartition(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .map(HoodieRecordIndex::isPartitioned)
        .orElse(false);
    return partitioned ? lookupPartitioned(keys, context) : lookupGlobal(keys, context);
  }

  private CandidateFiles lookupGlobal(List<String> keys, IndexLookupContext context) {
    Set<String> fileIds = readFileIds(keys, Option.empty(), context);
    return CandidateFiles.of(filesOf(context, fileIds, new HashSet<>()));
  }

  private CandidateFiles lookupPartitioned(List<String> keys, IndexLookupContext context) {
    Set<String> partitions = context.filesByPartition().keySet();
    if (partitions.isEmpty() || partitions.size() > MAX_PARTITIONS) {
      return CandidateFiles.all();
    }
    Map<String, List<FileSlice>> shardsByPartition = context.metadata().getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
    Set<String> fileIds = new HashSet<>();
    Set<String> notIndexed = new HashSet<>();
    boolean anyIndexed = false;
    for (String partition : partitions) {
      List<FileSlice> shards = shardsByPartition.get(partition);
      if (shards == null || shards.isEmpty()) {
        notIndexed.add(partition);
        continue;
      }
      anyIndexed = true;
      // a partitioned probe reads a single shard, so the keys go by shard
      Map<Integer, List<String>> keysByShard = new HashMap<>();
      for (String key : keys) {
        keysByShard.computeIfAbsent(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, shards.size()), s -> new ArrayList<>()).add(key);
      }
      for (List<String> shardKeys : keysByShard.values()) {
        fileIds.addAll(readFileIds(shardKeys, Option.of(partition), context));
      }
    }
    if (!anyIndexed) {
      return CandidateFiles.all();
    }
    return CandidateFiles.of(filesOf(context, fileIds, notIndexed));
  }

  private static Set<String> readFileIds(List<String> keys, Option<String> partition, IndexLookupContext context) {
    HoodieData<String> keyData = context.engineContext().parallelize(keys, Math.max(1, Math.min(keys.size(), 200)));
    HoodiePairData<String, HoodieRecordGlobalLocation> locations = context.metadata().readRecordIndexLocationsWithKeys(keyData, partition);
    try {
      Set<String> fileIds = new HashSet<>();
      for (Pair<String, HoodieRecordGlobalLocation> location : HoodieDataUtils.dedupeAndCollectAsList(locations)) {
        fileIds.add(location.getValue().getFileId());
      }
      return fileIds;
    } finally {
      locations.unpersistWithDependencies();
    }
  }

  /**
   * The candidate files whose file id is one of {@code fileIds}, plus the files of the partitions in
   * {@code keepPartitions} and the files whose file id cannot be parsed.
   */
  private static Set<String> filesOf(IndexLookupContext context, Set<String> fileIds, Set<String> keepPartitions) {
    Set<String> kept = new HashSet<>();
    for (Map.Entry<String, List<String>> partition : context.filesByPartition().entrySet()) {
      boolean keepAll = keepPartitions.contains(partition.getKey());
      for (String file : partition.getValue()) {
        Option<String> fileId = fileIdOf(file);
        if (keepAll || !fileId.isPresent() || fileIds.contains(fileId.get())) {
          kept.add(file);
        }
      }
    }
    return kept;
  }

  /**
   * The file id of a Hudi base or log file name, or empty if the name is not one.
   */
  static Option<String> fileIdOf(String fileName) {
    try {
      Option<FileNameParser.LogFileName> logFile = FileNameParser.parseLogFile(fileName);
      if (logFile.isPresent()) {
        return Option.of(logFile.get().getFileId());
      }
      Option<FileNameParser.BaseFileName> baseFile = FileNameParser.parseBaseFile(fileName);
      return baseFile.isPresent() ? Option.of(baseFile.get().getFileId()) : Option.empty();
    } catch (RuntimeException e) {
      return Option.empty();
    }
  }

  // -------------------------------------------------------------------------
  //  Record keys from the filter
  // -------------------------------------------------------------------------

  /**
   * The record keys to look up, or empty if the record index cannot be used for the filter.
   */
  List<String> recordKeys(Expression filter, HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Option<String[]> recordKeyFields = tableConfig.getRecordKeyFields();
    if (filter == null || !recordKeyFields.isPresent() || recordKeyFields.get().length == 0) {
      return new ArrayList<>();
    }
    String[] fields = recordKeyFields.get();
    boolean prefixedSingleField = false;
    if (tableConfig.isComplexKeyGenWithSingleRecordKeyField()) {
      Option<ComplexKeyGenEncoding> encoding = encodingResolver.apply(metaClient);
      if (!encoding.isPresent()) {
        // a single-field complex key whose encoding is unknown cannot be pruned by key
        return new ArrayList<>();
      }
      prefixedSingleField = encoding.get().encodesFieldName();
    }
    boolean complex = fields.length > 1 || prefixedSingleField;
    List<Expression> conjuncts = LookupUtils.conjuncts(filter);

    Set<String> metaFieldKeys = literalsFor(HoodieRecord.RECORD_KEY_METADATA_FIELD, conjuncts);
    if (!complex) {
      Set<String> keys = new LinkedHashSet<>(metaFieldKeys);
      keys.addAll(literalsFor(fields[0], conjuncts));
      return new ArrayList<>(keys);
    }
    if (!metaFieldKeys.isEmpty()) {
      // whole stored keys
      return new ArrayList<>(metaFieldKeys);
    }
    List<String> keys = new ArrayList<>();
    keys.add(null);
    for (String field : fields) {
      Set<String> values = literalsFor(field, conjuncts);
      if (values.isEmpty() || (long) keys.size() * values.size() > MAX_KEYS) {
        return new ArrayList<>();
      }
      List<String> combined = new ArrayList<>(keys.size() * values.size());
      for (String prefix : keys) {
        for (String value : values) {
          String part = field + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR + value;
          combined.add(prefix == null ? part : prefix + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + part);
        }
      }
      keys = combined;
    }
    return keys;
  }

  /**
   * The literals of the conjuncts {@code field = v} and {@code field IN (...)}; a conjunct with a literal that cannot
   * be used is ignored as a whole.
   */
  private static Set<String> literalsFor(String field, List<Expression> conjuncts) {
    Set<String> values = new LinkedHashSet<>();
    for (Expression conjunct : conjuncts) {
      List<Expression> literals = null;
      if (conjunct instanceof Predicates.BinaryComparison && ((Predicates.BinaryComparison) conjunct).getOperator() == Expression.Operator.EQ) {
        Predicates.BinaryComparison eq = (Predicates.BinaryComparison) conjunct;
        if (isField(eq.getLeft(), field)) {
          literals = Arrays.asList(eq.getRight());
        } else if (isField(eq.getRight(), field)) {
          literals = Arrays.asList(eq.getLeft());
        }
      } else if (conjunct instanceof Predicates.In && isField(conjunct.getChildren().get(0), field)) {
        literals = ((Predicates.In) conjunct).getRightChildren();
      }
      if (literals == null || literals.isEmpty()) {
        continue;
      }
      List<String> conjunctValues = new ArrayList<>();
      for (Expression literal : literals) {
        String value = keyValue(literal);
        if (value == null) {
          conjunctValues = null;
          break;
        }
        conjunctValues.add(value);
      }
      if (conjunctValues != null) {
        values.addAll(conjunctValues);
      }
    }
    return values;
  }

  private static boolean isField(Expression expr, String field) {
    return expr instanceof NameReference && field.equals(((NameReference) expr).getName());
  }

  /**
   * The key form of a literal, or null if it cannot be used.
   */
  private static String keyValue(Expression expr) {
    if (!(expr instanceof Literal)) {
      return null;
    }
    Literal<?> literal = (Literal<?>) expr;
    if (literal.getValue() == null || literal.getDataType() == null) {
      return null;
    }
    HoodieSchemaType type = literal.getDataType().getType();
    // CHAR literals are strings, and INT8 / INT16 literals Integers, whose string forms are the stored key as for
    // STRING and INT
    if (type.isStringLike() && literal.getValue() instanceof CharSequence) {
      String value = literal.getValue().toString();
      return value.isEmpty() ? null : value;
    }
    if ((type.isIntLike() && literal.getValue() instanceof Integer)
        || (type == HoodieSchemaType.LONG && literal.getValue() instanceof Long)) {
      return literal.getValue().toString();
    }
    return null;
  }

  /**
   * The record key encoding of a single-field complex key generator table as the table config gives it: the persisted
   * encoding, or, when the record key meta field is not populated, the encoding of the table version's key generator.
   * Empty when the encoding is not persisted (Spark then deduces it from the data).
   */
  public static Option<ComplexKeyGenEncoding> encodingFromTableConfig(HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (!tableConfig.isComplexKeyGenWithSingleRecordKeyField()) {
      return Option.empty();
    }
    if (!tableConfig.isRecordKeyPopulated()) {
      return tableConfig.getTableVersion().greaterThanOrEquals(HoodieTableVersion.NINE)
          ? Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED) : Option.empty();
    }
    return tableConfig.getComplexKeyGenEncoding();
  }
}
