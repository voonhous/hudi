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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.FieldSchema;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPartitionFilterGenerator {

  PartitionFilterGenerator partitionFilterGenerator = new PartitionFilterGenerator();
  @Test
  public void testPushDownFilters() {
    Properties props = new Properties();
    HiveSyncConfig config = new HiveSyncConfig(props);
    List<FieldSchema> partitionFieldSchemas = new ArrayList<>(4);
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("year", "string"));
    partitionFieldSchemas.add(new FieldSchema("month", "int"));
    partitionFieldSchemas.add(new FieldSchema("day", "bigint"));

    List<String> writtenPartitions = new ArrayList<>();
    writtenPartitions.add("2022-09-01/2022/9/1");
    assertEquals("(((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    writtenPartitions.add("2022-09-02/2022/9/2");
    assertEquals(
        "((((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1) OR (((date = 2022-09-02 AND year = \"2022\") AND month = 9) AND day = 2))",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If there are incompatible types to convert as filters inside partition
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("2022-09-01/true");
    assertEquals("date = 2022-09-01",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("2022-09-02/true");
    assertEquals("(date = 2022-09-01 OR date = 2022-09-02)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If no compatible types matched to convert as filters
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("true");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("false");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
  }

  @Test
  public void testFilterGenVisitorLiteralTypes() {
    FilterGenVisitor visitor = new FilterGenVisitor();
    // STRING is quoted; INT, LONG and DATE are printed as is.
    assertEquals("\"a\"", Literal.from("a").accept(visitor));
    assertEquals("'a\"b'", Literal.from("a\"b").accept(visitor));
    assertEquals("7", Literal.from(7).accept(visitor));
    assertEquals("7", Literal.from(7L).accept(visitor));
    assertEquals("2022-09-01", new Literal<>("2022-09-01", HoodieSchema.createDate()).accept(visitor));
    // Every other type is dropped from the filter, as are comparisons on it.
    assertEquals("", Literal.from(UUID.randomUUID()).accept(visitor));
    assertEquals("", Literal.from(new BigDecimal("1.5")).accept(visitor));
    assertEquals("", Literal.from(1.5d).accept(visitor));
    assertEquals("", Literal.from(true).accept(visitor));
    assertEquals("", new Literal<>(1L, HoodieSchema.createTimestampMicros()).accept(visitor));
    assertEquals("", new Literal<>("a", (HoodieSchema) null).accept(visitor));
    assertEquals("", Predicates.eq(new NameReference("b"), Literal.from(true)).accept(visitor));
    assertEquals("b = 1", Predicates.eq(new NameReference("b"), new Literal<>(1, HoodieSchema.create(HoodieSchemaType.INT))).accept(visitor));
  }

  @Test
  public void testMinMaxFilterNoNumericOverflow() {
    Properties props = new Properties();
    // force the min/max branch so the values are sorted by ValueComparator
    props.put(HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE.key(), "0");
    HiveSyncConfig config = new HiveSyncConfig(props);
    List<FieldSchema> partitionFieldSchemas = new ArrayList<>(2);
    partitionFieldSchemas.add(new FieldSchema("intcol", "int"));
    partitionFieldSchemas.add(new FieldSchema("bigcol", "bigint"));

    List<String> writtenPartitions = new ArrayList<>();
    // extreme values whose pairwise difference overflows int/long subtraction
    writtenPartitions.add("2147483647/9223372036854775807");
    writtenPartitions.add("-2147483648/-9223372036854775808");
    writtenPartitions.add("0/0");

    // bounds must reflect the true numeric order; a subtraction-based comparator overflows and
    // would pick wrong min/max (or throw a comparator-contract violation).
    assertEquals(
        "((intcol >= -2147483648 AND intcol <= 2147483647) "
            + "AND (bigcol >= -9223372036854775808 AND bigcol <= 9223372036854775807))",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
  }

  @Test
  public void testPushDownFilterIfExceedLimit() {
    Properties props = new Properties();
    props.put(HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE.key(), "0");
    HiveSyncConfig config = new HiveSyncConfig(props);
    List<FieldSchema> partitionFieldSchemas = new ArrayList<>(4);
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("year", "string"));
    partitionFieldSchemas.add(new FieldSchema("month", "int"));
    partitionFieldSchemas.add(new FieldSchema("day", "bigint"));

    List<String> writtenPartitions = new ArrayList<>();
    writtenPartitions.add("2022-09-01/2022/9/1");

    assertEquals("(((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    writtenPartitions.add("2022-09-02/2022/9/2");
    writtenPartitions.add("2022-09-03/2022/9/2");
    writtenPartitions.add("2022-09-04/2022/9/2");
    assertEquals(
        "((((date >= 2022-09-01 AND date <= 2022-09-04) AND year = \"2022\") AND month = 9) AND (day >= 1 AND day <= 2))",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If there are incompatible types to convert as filters inside partition
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("2022-09-01/true");
    assertEquals("date = 2022-09-01",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("2022-09-02/true");
    writtenPartitions.add("2022-09-03/false");
    writtenPartitions.add("2022-09-04/false");
    assertEquals("(date >= 2022-09-01 AND date <= 2022-09-04)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If no compatible types matched to convert as filters
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("true");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("false");
    writtenPartitions.add("false");
    writtenPartitions.add("false");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
  }
}
