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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.SchemaMappers;
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link BigQueryRenderer} produces what {@link BigQuerySchemaResolver#getTableSchema} produces today.
 */
class TestBigQueryRenderer {

  private static final BigQueryRenderer RENDERER = new BigQueryRenderer();

  /**
   * The types BigQuery sync maps specially: INT and FLOAT widened, UUID as STRING, every DECIMAL as NUMERIC, VECTOR as
   * BYTES, maps as key_value structs, arrays as REPEATED, a NULL column (skipped) and partition columns.
   */
  private static HoodieSchema tableSchema() {
    HoodieSchema nested = HoodieSchema.createRecord("nested", "test", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    return HoodieSchema.createRecord("trips", "test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("dt", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("hour", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("score", HoodieSchema.create(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("ts", HoodieSchema.createTimestampMillis()),
        HoodieSchemaField.of("local_ts", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMicros())),
        HoodieSchemaField.of("price", HoodieSchema.createDecimal(10, 2)),
        HoodieSchemaField.of("big_price", HoodieSchema.createDecimal("big", "test", null, 38, 10, 16)),
        HoodieSchemaField.of("uid", HoodieSchema.createUUID()),
        HoodieSchemaField.of("emb", HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.FLOAT)),
        HoodieSchemaField.of("tags", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("attrs", HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchemaType.INT))),
        HoodieSchemaField.of("nested", HoodieSchema.createNullable(nested)),
        HoodieSchemaField.of("nothing", HoodieSchema.create(HoodieSchemaType.NULL))));
  }

  @ParameterizedTest
  @CsvSource({"''", "dt", "dt;hour", "dt;not_a_column"})
  void renderEqualsGetTableSchema(String partitions) throws Exception {
    List<String> partitionFields = partitions.isEmpty() ? Collections.emptyList() : Arrays.asList(partitions.split(";"));
    assertEquals(today(tableSchema(), partitionFields), RENDERER.render(tableSchema(), partitionFields, SyncOptions.DEFAULT));
  }

  @Test
  void renderMatchesTodaysTypes() throws Exception {
    Schema rendered = RENDERER.render(tableSchema(), Collections.singletonList("dt"), SyncOptions.DEFAULT);
    assertEquals(today(tableSchema(), Collections.singletonList("dt")), rendered);
    assertEquals(StandardSQLTypeName.INT64, rendered.getFields().get("hour").getType().getStandardType());
    assertEquals(StandardSQLTypeName.FLOAT64, rendered.getFields().get("score").getType().getStandardType());
    assertEquals(StandardSQLTypeName.STRING, rendered.getFields().get("uid").getType().getStandardType());
    assertEquals(StandardSQLTypeName.NUMERIC, rendered.getFields().get("price").getType().getStandardType());
    assertEquals(StandardSQLTypeName.NUMERIC, rendered.getFields().get("big_price").getType().getStandardType());
    assertNull(rendered.getFields().get("big_price").getScale());
    assertEquals(StandardSQLTypeName.BYTES, rendered.getFields().get("emb").getType().getStandardType());
    assertEquals(Field.Mode.REPEATED, rendered.getFields().get("tags").getMode());
    assertEquals(12, rendered.getFields().size()); // 14 fields, minus the partition column and the skipped NULL column
  }

  @Test
  void existingResolverFixtures() throws Exception {
    // A table whose only nested record holds a NULL column, and one whose map holds records: today's test inputs.
    HoodieSchema nestedNull = HoodieSchema.parse("{\"type\": \"record\", \"name\": \"testRecord\", \"fields\": ["
        + "{\"name\": \"name\", \"type\": \"string\"},"
        + "{\"name\": \"address\", \"type\": {\"type\": \"record\", \"name\": \"address\", \"fields\": ["
        + "{\"name\": \"city\", \"type\": \"string\"},"
        + "{\"name\": \"nestedpromotion\", \"type\": \"null\", \"default\": null}]}}]}");
    assertEquals(today(nestedNull, Collections.emptyList()), RENDERER.render(nestedNull, Collections.emptyList(), SyncOptions.DEFAULT));
    HoodieSchema recordMap = HoodieSchema.createRecord("testRecord", null, null, false, Collections.singletonList(
        HoodieSchemaField.of("recordMap", HoodieSchema.createNullable(HoodieSchema.createMap(
            HoodieSchema.createRecord("element", null, null, false, Arrays.asList(
                HoodieSchemaField.of("requiredDouble", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
                HoodieSchemaField.of("optionalString", HoodieSchema.createNullable(HoodieSchemaType.STRING)))))))));
    assertEquals(today(recordMap, Collections.emptyList()), RENDERER.render(recordMap, Collections.emptyList(), SyncOptions.DEFAULT));
  }

  @Test
  void diffIsNotSupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> RENDERER.diff(tableSchema(), Schema.of(), Collections.emptyList(), SyncOptions.DEFAULT));
  }

  @Test
  void isRegistered() {
    assertTrue(CatalogSchemaRenderers.catalogs().contains(CatalogSchemaRenderers.BIGQUERY));
    CatalogSchemaRenderer<Schema> renderer = CatalogSchemaRenderers.get(CatalogSchemaRenderers.BIGQUERY);
    assertInstanceOf(BigQueryRenderer.class, renderer);
    assertInstanceOf(BigQueryTypeMapper.class, SchemaMappers.typeSystem(TypeSystems.BIGQUERY));
  }

  /**
   * The resolver skips a NULL column without failing, so the mapper's UNSUPPORTED declaration for NULL is enforced
   * only when the record has no other column (BigQuery's client rejects an empty STRUCT).
   */
  @Test
  void nullColumnsAreSkipped() {
    BigQueryTypeMapper mapper = new BigQueryTypeMapper();
    HoodieSchema withNull = HoodieSchema.createRecord("r", null, null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.NULL))));
    assertEquals(1, mapper.toTypeSystem(withNull).getSubFields().size());
    HoodieSchema onlyNull = HoodieSchema.createRecord("r", null, null,
        Collections.singletonList(HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.NULL))));
    assertThrows(IllegalArgumentException.class, () -> mapper.toTypeSystem(onlyNull));
  }

  private static Schema today(HoodieSchema schema, List<String> partitionFields) throws Exception {
    TableSchemaResolver tableSchemaResolver = mock(TableSchemaResolver.class);
    when(tableSchemaResolver.getTableSchema()).thenReturn(schema);
    return new BigQuerySchemaResolver(metaClient -> tableSchemaResolver).getTableSchema(mock(HoodieTableMetaClient.class), partitionFields);
  }
}
