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
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the plan order and the Spark-like execution of {@link DefaultIndexPlanner}, and the record key extraction of
 * {@link RecordIndexLookup}.
 */
public class TestDefaultIndexPlanner {

  private static final HoodieSchema STRING = HoodieSchema.create(HoodieSchemaType.STRING);
  private static final HoodieSchema INT = HoodieSchema.create(HoodieSchemaType.INT);
  private static final Expression FILTER = Predicates.eq(new NameReference("a"), new Literal<>(1, INT));

  /**
   * A lookup that keeps fixed files, and records what it saw.
   */
  private static final class FakeLookup implements IndexLookup {
    private final MetadataPartitionType index;
    private final boolean canPrune;
    private final CandidateFiles result;
    private final List<Set<String>> seen = new ArrayList<>();

    FakeLookup(MetadataPartitionType index, boolean canPrune, CandidateFiles result) {
      this.index = index;
      this.canPrune = canPrune;
      this.result = result;
    }

    @Override
    public MetadataPartitionType index() {
      return index;
    }

    @Override
    public boolean canPrune(Expression filter, IndexLookupContext context) {
      return canPrune;
    }

    @Override
    public CandidateFiles lookup(Expression filter, IndexLookupContext context) {
      seen.add(LookupUtils.allFiles(context));
      return result;
    }
  }

  private static CandidateFiles files(String... names) {
    return CandidateFiles.of(new HashSet<>(Arrays.asList(names)));
  }

  private static IndexLookupContext context() {
    Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
    filesByPartition.put("p1", Arrays.asList("f1", "f2"));
    filesByPartition.put("p2", Arrays.asList("f3", "f4"));
    return new IndexLookupContext(null, null, null, null, HudiStatsEvaluator.getInstance(), filesByPartition);
  }

  @Test
  void planOrder() {
    DefaultIndexPlanner planner = new DefaultIndexPlanner();
    List<IndexLookup> plan = planner.plan(FILTER, null, EnumSet.of(MetadataPartitionType.COLUMN_STATS,
        MetadataPartitionType.RECORD_INDEX, MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.BLOOM_FILTERS));
    assertEquals(Arrays.asList(MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.RECORD_INDEX, MetadataPartitionType.COLUMN_STATS),
        Arrays.asList(plan.get(0).index(), plan.get(1).index(), plan.get(2).index()));
    assertEquals(1, planner.plan(FILTER, null, EnumSet.of(MetadataPartitionType.COLUMN_STATS)).size());
    assertTrue(planner.plan(FILTER, null, EnumSet.of(MetadataPartitionType.FILES)).isEmpty());
    assertTrue(planner.plan(null, null, EnumSet.allOf(MetadataPartitionType.class)).isEmpty());
  }

  @Test
  void firstFileLevelLookupThatPrunesWins() {
    FakeLookup cannot = new FakeLookup(MetadataPartitionType.RECORD_INDEX, false, files("f1"));
    FakeLookup all = new FakeLookup(MetadataPartitionType.RECORD_INDEX, true, CandidateFiles.all());
    FakeLookup first = new FakeLookup(MetadataPartitionType.COLUMN_STATS, true, files("f2", "f3"));
    FakeLookup second = new FakeLookup(MetadataPartitionType.COLUMN_STATS, true, files("f3"));
    CandidateFiles result = DefaultIndexPlanner.lookup(Arrays.asList(cannot, all, first, second), FILTER, context());
    assertEquals(files("f2", "f3"), result);
    assertTrue(cannot.seen.isEmpty());
    assertEquals(1, all.seen.size());
    assertTrue(second.seen.isEmpty());
  }

  @Test
  void partitionLevelLookupsNarrowFirst() {
    FakeLookup columnStats = new FakeLookup(MetadataPartitionType.COLUMN_STATS, true, files("f2", "f3"));
    FakeLookup partitionStats = new FakeLookup(MetadataPartitionType.PARTITION_STATS, true, files("f1", "f2"));
    // the partition-level lookup runs first even when listed later, and the file-level one only sees its files
    CandidateFiles result = DefaultIndexPlanner.lookup(Arrays.asList(columnStats, partitionStats), FILTER, context());
    assertEquals(files("f2"), result);
    assertEquals(Collections.singletonList(new HashSet<>(Arrays.asList("f1", "f2"))), columnStats.seen);

    // no file-level result: the partition-level one stands
    FakeLookup none = new FakeLookup(MetadataPartitionType.COLUMN_STATS, false, files());
    assertEquals(files("f1", "f2"), DefaultIndexPlanner.lookup(Arrays.asList(partitionStats, none), FILTER, context()));
    // nothing prunes
    assertTrue(DefaultIndexPlanner.lookup(Collections.singletonList(none), FILTER, context()).isAll());
  }

  @Test
  void lookupAllIntersects() {
    FakeLookup recordIndex = new FakeLookup(MetadataPartitionType.RECORD_INDEX, true, files("f1", "f2", "f3"));
    FakeLookup columnStats = new FakeLookup(MetadataPartitionType.COLUMN_STATS, true, files("f2", "f3"));
    FakeLookup partitionStats = new FakeLookup(MetadataPartitionType.PARTITION_STATS, true, files("f1", "f2", "f4"));
    CandidateFiles result = DefaultIndexPlanner.lookupAll(Arrays.asList(recordIndex, columnStats, partitionStats), FILTER, context());
    assertEquals(files("f2"), result);
    // each lookup sees what the previous ones kept
    assertEquals(Collections.singletonList(new HashSet<>(Arrays.asList("f1", "f2", "f4"))), recordIndex.seen);
    assertEquals(Collections.singletonList(new HashSet<>(Arrays.asList("f1", "f2"))), columnStats.seen);
    assertTrue(DefaultIndexPlanner.lookupAll(Collections.emptyList(), FILTER, context()).isAll());
  }

  // -------------------------------------------------------------------------
  //  Record keys
  // -------------------------------------------------------------------------

  private static HoodieTableMetaClient metaClient(String[] recordKeyFields, boolean complexSingleField) {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.ofNullable(recordKeyFields));
    when(tableConfig.isComplexKeyGenWithSingleRecordKeyField()).thenReturn(complexSingleField);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    return metaClient;
  }

  private static Expression eq(String field, Object value, HoodieSchema type) {
    return Predicates.eq(new NameReference(field), new Literal<>(value, type));
  }

  private static Expression in(String field, String... values) {
    List<Expression> literals = new ArrayList<>();
    for (String value : values) {
      literals.add(new Literal<>(value, STRING));
    }
    return Predicates.in(new NameReference(field), literals);
  }

  @Test
  void simpleKeys() {
    RecordIndexLookup lookup = new RecordIndexLookup();
    HoodieTableMetaClient simple = metaClient(new String[] {"id"}, false);
    assertEquals(Arrays.asList("k1"), lookup.recordKeys(eq("id", "k1", STRING), simple));
    // literal on the left, other conjuncts, IN, and the meta field: united
    Expression filter = Predicates.and(
        Predicates.and(Predicates.eq(new Literal<>("k1", STRING), new NameReference("id")), Predicates.gt(new NameReference("x"), new Literal<>(1, INT))),
        Predicates.and(in("id", "k2", "k3"), eq(HoodieRecord.RECORD_KEY_METADATA_FIELD, "k0", STRING)));
    assertEquals(new HashSet<>(Arrays.asList("k0", "k1", "k2", "k3")), new HashSet<>(lookup.recordKeys(filter, simple)));
    assertEquals(Arrays.asList("7"), lookup.recordKeys(eq("id", 7, INT), simple));
    assertEquals(Arrays.asList("7"), lookup.recordKeys(eq("id", 7L, HoodieSchema.create(HoodieSchemaType.LONG)), simple));
    // INT8 / INT16 literals key as INT, CHAR literals as STRING (the value as given, padding included)
    assertEquals(Arrays.asList("7"), lookup.recordKeys(eq("id", 7, HoodieSchema.createInt8()), simple));
    assertEquals(Arrays.asList("300"), lookup.recordKeys(eq("id", 300, HoodieSchema.createInt16()), simple));
    assertEquals(Arrays.asList("k1 "), lookup.recordKeys(eq("id", "k1 ", HoodieSchema.createChar(3)), simple));
    assertTrue(lookup.recordKeys(eq("id", "", HoodieSchema.createChar(3)), simple).isEmpty());
    // not usable: OR, NOT, other columns, a date literal, an empty string, a null
    assertTrue(lookup.recordKeys(Predicates.or(eq("id", "k1", STRING), eq("id", "k2", STRING)), simple).isEmpty());
    assertTrue(lookup.recordKeys(Predicates.not(eq("id", "k1", STRING)), simple).isEmpty());
    assertTrue(lookup.recordKeys(eq("other", "k1", STRING), simple).isEmpty());
    assertTrue(lookup.recordKeys(eq("id", 19000, HoodieSchema.createDate()), simple).isEmpty());
    assertTrue(lookup.recordKeys(eq("id", "", STRING), simple).isEmpty());
    assertTrue(lookup.recordKeys(eq("id", null, STRING), simple).isEmpty());
    assertTrue(lookup.recordKeys(Predicates.lt(new NameReference("id"), new Literal<>("k", STRING)), simple).isEmpty());
    // no record key fields
    assertTrue(lookup.recordKeys(eq("id", "k1", STRING), metaClient(null, false)).isEmpty());
  }

  @Test
  void complexKeys() {
    RecordIndexLookup lookup = new RecordIndexLookup();
    HoodieTableMetaClient twoFields = metaClient(new String[] {"a", "b"}, false);
    Expression filter = Predicates.and(in("a", "1", "2"), eq("b", "x", STRING));
    assertEquals(Arrays.asList("a:1,b:x", "a:2,b:x"), lookup.recordKeys(filter, twoFields));
    // every field needs literals
    assertTrue(lookup.recordKeys(eq("a", "1", STRING), twoFields).isEmpty());
    // whole keys on the meta field
    assertEquals(Arrays.asList("a:1,b:x"), lookup.recordKeys(eq(HoodieRecord.RECORD_KEY_METADATA_FIELD, "a:1,b:x", STRING), twoFields));

    // single-field complex keys follow the encoding
    HoodieTableMetaClient single = metaClient(new String[] {"a"}, true);
    assertEquals(Arrays.asList("a:1"), new RecordIndexLookup(m -> Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED)).recordKeys(eq("a", "1", STRING), single));
    assertEquals(Arrays.asList("1"), new RecordIndexLookup(m -> Option.of(ComplexKeyGenEncoding.VALUE_ONLY)).recordKeys(eq("a", "1", STRING), single));
    assertTrue(new RecordIndexLookup(m -> Option.empty()).recordKeys(eq("a", "1", STRING), single).isEmpty());
  }

  @Test
  void fileIds() {
    assertEquals(Option.of("136281f3-c24e-423b-a65a-95dbfbddce1d-0"),
        RecordIndexLookup.fileIdOf("136281f3-c24e-423b-a65a-95dbfbddce1d-0_1-0-1_20240706120100123.parquet"));
    assertEquals(Option.of("136281f3-c24e-423b-a65a-95dbfbddce1d-0"),
        RecordIndexLookup.fileIdOf(".136281f3-c24e-423b-a65a-95dbfbddce1d-0_20240706120100123.log.1_0-1-2"));
    assertEquals(Option.empty(), RecordIndexLookup.fileIdOf("plain.parquet"));
  }
}
