/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.expression.{ArrayData, BindVisitor}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.schema.internal.utils.Conversions

import org.apache.spark.sql.sources.{And, EqualTo, GreaterThan, StringStartsWith}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._

/**
 * Tests the [[HoodieSchema]] types [[SparkFilterHelper]] gives partition schemas and filter literals.
 */
class TestSparkFilterHelperTypes {

  @Test
  def testConvertDataTypeMappingTable(): Unit = {
    val expected: Seq[(DataType, HoodieSchema)] = Seq(
      BooleanType -> HoodieSchema.create(HoodieSchemaType.BOOLEAN),
      IntegerType -> HoodieSchema.create(HoodieSchemaType.INT),
      ShortType -> HoodieSchema.create(HoodieSchemaType.INT),
      ByteType -> HoodieSchema.create(HoodieSchemaType.INT),
      LongType -> HoodieSchema.create(HoodieSchemaType.LONG),
      FloatType -> HoodieSchema.create(HoodieSchemaType.FLOAT),
      DoubleType -> HoodieSchema.create(HoodieSchemaType.DOUBLE),
      StringType -> HoodieSchema.create(HoodieSchemaType.STRING),
      CharType(3) -> HoodieSchema.create(HoodieSchemaType.STRING),
      VarcharType(3) -> HoodieSchema.create(HoodieSchemaType.STRING),
      DateType -> HoodieSchema.createDate(),
      TimestampType -> HoodieSchema.createTimestampMicros(),
      DecimalType(10, 2) -> HoodieSchema.createDecimal(10, 2))
    expected.foreach { case (sparkType, hoodieSchema) =>
      assertEquals(hoodieSchema, SparkFilterHelper.convertDataType(sparkType), s"$sparkType")
    }
  }

  @Test
  def testConvertDataTypeUnsupportedTypesThrow(): Unit = {
    Seq(BinaryType, ArrayType(IntegerType), MapType(StringType, IntegerType), TimestampNTZType).foreach { sparkType =>
      assertThrows(classOf[UnsupportedOperationException], () => {
        SparkFilterHelper.convertDataType(sparkType)
        ()
      })
    }
  }

  @Test
  def testConvertDataTypeStruct(): Unit = {
    val struct = StructType(Seq(
      StructField("region", StringType, nullable = true),
      StructField("day", DateType, nullable = false),
      StructField("hh", IntegerType, nullable = true).withComment("hour")))
    val record = SparkFilterHelper.convertDataType(struct)

    assertEquals(HoodieSchemaType.RECORD, record.getType)
    val fields = record.getFields.asScala
    assertEquals(Seq("region", "day", "hh"), fields.map(_.name()))
    assertEquals(Seq(0, 1, 2), fields.map(_.pos()))
    assertEquals(HoodieSchema.createNullable(HoodieSchemaType.STRING), fields.head.schema())
    assertEquals(HoodieSchema.createDate(), fields(1).schema())
    assertEquals(HoodieSchema.createNullable(HoodieSchemaType.INT), fields(2).schema())
    assertEquals("hour", fields(2).doc().get())
    assertTrue(Conversions.isPartitionSchemaSupportedConversion(record))

    val withTimestamp = SparkFilterHelper.convertDataType(StructType(Seq(StructField("ts", TimestampType))))
    assertFalse(Conversions.isPartitionSchemaSupportedConversion(withTimestamp))

    // Nested structs get distinct record names.
    val nested = SparkFilterHelper.convertDataType(StructType(Seq(
      StructField("a", StructType(Seq(StructField("x", IntegerType)))),
      StructField("b", StructType(Seq(StructField("x", IntegerType)))))))
    assertEquals(2, nested.getFields.size())
  }

  @Test
  def testToLiteralTypes(): Unit = {
    assertEquals(HoodieSchema.createTimestampMicros(), SparkFilterHelper.toLiteral(new Timestamp(1000L)).getDataType)
    assertEquals(1000000L, SparkFilterHelper.toLiteral(new Timestamp(1000L)).getValue)
    assertEquals(HoodieSchema.createDate(), SparkFilterHelper.toLiteral(Date.valueOf("1970-01-02")).getDataType)
    assertEquals(1, SparkFilterHelper.toLiteral(Date.valueOf("1970-01-02")).getValue)
    assertEquals(HoodieSchema.createTimestampMicros(), SparkFilterHelper.toLiteral(Instant.ofEpochSecond(1)).getDataType)
    assertEquals(1000000L, SparkFilterHelper.toLiteral(Instant.ofEpochSecond(1)).getValue)
    // RFC-88 bug list: a LocalDate holds epoch days yet is typed as a timestamp.
    assertEquals(HoodieSchema.createTimestampMicros(), SparkFilterHelper.toLiteral(LocalDate.ofEpochDay(3)).getDataType)
    assertEquals(3, SparkFilterHelper.toLiteral(LocalDate.ofEpochDay(3)).getValue)
    assertEquals(HoodieSchema.create(HoodieSchemaType.STRING), SparkFilterHelper.toLiteral("a").getDataType)
    assertEquals(HoodieSchema.create(HoodieSchemaType.INT), SparkFilterHelper.toLiteral(1).getDataType)
  }

  @Test
  def testConvertedFiltersBindAndEvalOnConvertedPartitionSchema(): Unit = {
    val partitionSchema = SparkFilterHelper.convertDataType(StructType(Seq(
      StructField("Region", StringType, nullable = true),
      StructField("hh", IntegerType, nullable = true))))
    val filter = SparkFilterHelper.convertFilters(Seq(
      And(StringStartsWith("region", "us"), GreaterThan("hh", 10)),
      EqualTo("HH", 12)))
    val bound = filter.accept(new BindVisitor(partitionSchema, false))

    assertTrue(bound.eval(new ArrayData(List[AnyRef]("us-east", Int.box(12)).asJava)).asInstanceOf[Boolean])
    assertFalse(bound.eval(new ArrayData(List[AnyRef]("eu-west", Int.box(12)).asJava)).asInstanceOf[Boolean])
    assertFalse(bound.eval(new ArrayData(List[AnyRef]("us-east", Int.box(11)).asJava)).asInstanceOf[Boolean])
  }
}
