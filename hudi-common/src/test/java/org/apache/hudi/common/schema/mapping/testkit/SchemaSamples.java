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

package org.apache.hudi.common.schema.mapping.testkit;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The canonical column types every mapper is checked against, and the record shapes each one is wrapped in.
 *
 * <p>There is one sample per {@link HoodieSchemaType}, plus extra samples where a type's parameters change the
 * mapping (decimal backing, time and timestamp precision, local timestamps, vector element types). A type added to
 * {@code HoodieSchemaType} must get a sample here; {@code TestSchemaSamples} enforces that.
 */
public final class SchemaSamples {

  /**
   * Name of the column every sample is placed in.
   */
  public static final String COLUMN = "col";
  public static final String RECORD_NAME = "hoodie_mapping_test";
  public static final String RECORD_NAMESPACE = "hoodie.mapping.test";

  private static final List<SchemaSample> ALL = Collections.unmodifiableList(build());

  private SchemaSamples() {
  }

  public static List<SchemaSample> all() {
    return ALL;
  }

  public static SchemaSample get(String id) {
    return ALL.stream().filter(s -> s.id().equals(id)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown sample " + id));
  }

  public static Set<String> ids() {
    return ALL.stream().map(SchemaSample::id).collect(Collectors.toSet());
  }

  /**
   * Types with at least one sample.
   */
  public static Set<HoodieSchemaType> coveredTypes() {
    Set<HoodieSchemaType> types = EnumSet.noneOf(HoodieSchemaType.class);
    ALL.forEach(s -> types.add(s.type()));
    return types;
  }

  /**
   * A record with a single column {@value #COLUMN} of the given schema.
   */
  public static HoodieSchema record(HoodieSchema column) {
    return HoodieSchema.createRecord(RECORD_NAME, RECORD_NAMESPACE, null,
        Collections.singletonList(HoodieSchemaField.of(COLUMN, column)));
  }

  /**
   * Every shape a sample is checked in, as a top-level record schema with column {@value #COLUMN}.
   */
  public static List<SchemaCase> cases(SchemaSample sample) {
    List<SchemaCase> cases = new ArrayList<>();
    HoodieSchema s = sample.schema();
    if (sample.type() == HoodieSchemaType.NULL) {
      // A record whose only column is NULL is empty in most targets, which fails for the wrong reason.
      cases.add(new SchemaCase(sample, sample.id(), HoodieSchema.createRecord(RECORD_NAME, RECORD_NAMESPACE, null, Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)), HoodieSchemaField.of(COLUMN, s)))));
      return cases;
    }
    cases.add(new SchemaCase(sample, sample.id(), record(s)));
    if (sample.nesting() != SchemaSample.Nesting.NONE) {
      cases.add(new SchemaCase(sample, sample.id() + ".nullable", record(HoodieSchema.createNullable(s))));
    }
    if (sample.nesting() == SchemaSample.Nesting.ANY) {
      cases.add(new SchemaCase(sample, sample.id() + ".in_struct", record(HoodieSchema.createRecord("nested", RECORD_NAMESPACE + ".nested",
          null, Collections.singletonList(HoodieSchemaField.of("inner", s))))));
      cases.add(new SchemaCase(sample, sample.id() + ".in_array", record(HoodieSchema.createArray(s))));
      cases.add(new SchemaCase(sample, sample.id() + ".in_map", record(HoodieSchema.createMap(s))));
    }
    return cases;
  }

  public static List<SchemaCase> allCases() {
    return ALL.stream().flatMap(s -> cases(s).stream()).collect(Collectors.toList());
  }

  private static List<SchemaSample> build() {
    List<SchemaSample> samples = new ArrayList<>();
    samples.add(nestable("boolean", HoodieSchema.create(HoodieSchemaType.BOOLEAN)));
    samples.add(nestable("int", HoodieSchema.create(HoodieSchemaType.INT)));
    samples.add(nestable("long", HoodieSchema.create(HoodieSchemaType.LONG)));
    samples.add(nestable("float", HoodieSchema.create(HoodieSchemaType.FLOAT)));
    samples.add(nestable("double", HoodieSchema.create(HoodieSchemaType.DOUBLE)));
    samples.add(nestable("string", HoodieSchema.create(HoodieSchemaType.STRING)));
    samples.add(nestable("bytes", HoodieSchema.create(HoodieSchemaType.BYTES)));
    samples.add(nestable("fixed_16", HoodieSchema.createFixed("fixed_16", RECORD_NAMESPACE, null, 16)));
    samples.add(nestable("enum", HoodieSchema.createEnum("color", RECORD_NAMESPACE, null, Arrays.asList("RED", "GREEN", "BLUE"))));
    samples.add(nestable("decimal_10_2_bytes", HoodieSchema.createDecimal(10, 2)));
    samples.add(nestable("decimal_10_2_fixed_5", HoodieSchema.createDecimal("decimal_10_2", RECORD_NAMESPACE, null, 10, 2, 5)));
    samples.add(nestable("decimal_38_10_fixed_16", HoodieSchema.createDecimal("decimal_38_10", RECORD_NAMESPACE, null, 38, 10, 16)));
    samples.add(nestable("date", HoodieSchema.createDate()));
    samples.add(nestable("time_millis", HoodieSchema.createTimeMillis()));
    samples.add(nestable("time_micros", HoodieSchema.createTimeMicros()));
    samples.add(nestable("timestamp_millis", HoodieSchema.createTimestampMillis()));
    samples.add(nestable("timestamp_micros", HoodieSchema.createTimestampMicros()));
    samples.add(nestable("local_timestamp_millis", HoodieSchema.createLocalTimestampMillis()));
    samples.add(nestable("local_timestamp_micros", HoodieSchema.createLocalTimestampMicros()));
    samples.add(nestable("uuid", HoodieSchema.createUUID()));
    samples.add(nestable("array_int", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))));
    samples.add(nestable("map_long", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG))));
    samples.add(nestable("struct", HoodieSchema.createRecord("struct", RECORD_NAMESPACE + ".struct", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchemaType.STRING))))));
    samples.add(nestable("union_int_string", HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.STRING))));
    samples.add(nestable("variant", HoodieSchema.createVariant()));
    samples.add(nestable("blob", HoodieSchema.createBlob()));
    samples.add(topLevel("vector_float_128", HoodieSchema.createVector(128, HoodieSchema.Vector.VectorElementType.FLOAT)));
    samples.add(topLevel("vector_double_16", HoodieSchema.createVector(16, HoodieSchema.Vector.VectorElementType.DOUBLE)));
    samples.add(topLevel("vector_int8_32", HoodieSchema.createVector(32, HoodieSchema.Vector.VectorElementType.INT8)));
    // A NULL column has no nullable or nested form worth checking.
    samples.add(new SchemaSample("null", HoodieSchema.create(HoodieSchemaType.NULL), SchemaSample.Nesting.NONE));
    return samples;
  }

  private static SchemaSample nestable(String id, HoodieSchema schema) {
    return new SchemaSample(id, schema, SchemaSample.Nesting.ANY);
  }

  private static SchemaSample topLevel(String id, HoodieSchema schema) {
    return new SchemaSample(id, schema, SchemaSample.Nesting.TOP_LEVEL);
  }

  /**
   * One sample in one shape: the record schema handed to the mapper.
   */
  public static final class SchemaCase {
    private final SchemaSample sample;
    private final String id;
    private final HoodieSchema schema;

    SchemaCase(SchemaSample sample, String id, HoodieSchema schema) {
      this.sample = sample;
      this.id = id;
      this.schema = schema;
    }

    public SchemaSample sample() {
      return sample;
    }

    /**
     * {@code <sample id>} or {@code <sample id>.<shape>}, e.g. {@code uuid.in_map}.
     */
    public String id() {
      return id;
    }

    public HoodieSchema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return id;
    }
  }
}
