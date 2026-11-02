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

import org.apache.hudi.common.expression.{Expression, Literal, NameReference, Predicates}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._

object SparkFilterHelper {

  def convertFilters(filters: Seq[Filter]): Expression = {
    filters.flatMap(convertFilter)
      .reduceLeftOption(Predicates.and)
      .getOrElse(Predicates.alwaysTrue())
  }

  def convertFilter(filter: Filter): Option[Expression] = filter match {
    case EqualTo(attribute, value) =>
      Some(Predicates.eq(new NameReference(attribute), toLiteral(value)))
    case EqualNullSafe(attribute, value) =>
      Some(Predicates.eq(new NameReference(attribute), toLiteral(value)))
    case LessThan(attribute, value) =>
      Some(Predicates.lt(new NameReference(attribute), toLiteral(value)))
    case LessThanOrEqual(attribute, value) =>
      Some(Predicates.lteq(new NameReference(attribute), toLiteral(value)))
    case GreaterThan(attribute, value) =>
      Some(Predicates.gt(new NameReference(attribute), toLiteral(value)))
    case GreaterThanOrEqual(attribute, value) =>
      Some(Predicates.gteq(new NameReference(attribute), toLiteral(value)))
    case In(attribute, values) =>
      Some(Predicates.in(new NameReference(attribute), values.map(toLiteral(_).asInstanceOf[Expression]).toList.asJava))
    case And(left, right) =>
      for {
        convertedLeft <- convertFilter(left)
        convertedRight <- convertFilter(right)
      } yield Predicates.and(convertedLeft, convertedRight)
    case Or(left, right) =>
      for {
        convertedLeft <- convertFilter(left)
        convertedRight <- convertFilter(right)
      } yield Predicates.or(convertedLeft, convertedRight)
    case StringStartsWith(attribute, value) =>
      Some(Predicates.startsWith(new NameReference(attribute), toLiteral(value)))
    case StringContains(attribute, value) =>
      Some(Predicates.contains(new NameReference(attribute), toLiteral(value)))
    case Not(child) =>
      convertFilter(child).map(Predicates.not)
    case IsNull(attribute) =>
      Some(Predicates.isNull(new NameReference(attribute)))
    case IsNotNull(attribute) =>
      Some(Predicates.isNotNull(new NameReference(attribute)))
    case _ =>
      None
  }

  def toLiteral(value: Any): Literal[_] = {
    value match {
      case timestamp : Timestamp =>
        new Literal(DateTimeUtils.fromJavaTimestamp(timestamp), HoodieSchema.createTimestampMicros())
      case date: Date =>
        new Literal(DateTimeUtils.fromJavaDate(date), HoodieSchema.createDate())
      case instant: Instant =>
        new Literal(DateTimeUtils.instantToMicros(instant), HoodieSchema.createTimestampMicros())
      case localDate: LocalDate =>
        // RFC-88 bug list: a LocalDate holds epoch days but is typed as a timestamp-micros literal; it
        // should be typed as a date, like java.sql.Date above.
        new Literal(Math.toIntExact(localDate.toEpochDay), HoodieSchema.createTimestampMicros())
      case _ =>
        Literal.from(value)
    }
  }

  /**
   * Converts a Spark type into the [[HoodieSchema]] Hudi expressions are typed and bound with. A
   * [[StructType]] becomes a record whose fields keep their positions, nullable fields become nullable
   * unions; only the types below are supported.
   */
  def convertDataType(sparkType: DataType): HoodieSchema = convertDataType(sparkType, "record")

  private def convertDataType(sparkType: DataType, recordName: String): HoodieSchema = sparkType match {
    case StructType(fields) =>
      val convertedFields = fields.map { field =>
        val fieldSchema = convertDataType(field.dataType, s"${recordName}_${field.name}")
        HoodieSchemaField.of(field.name, if (field.nullable) HoodieSchema.createNullable(fieldSchema) else fieldSchema,
          field.getComment().orNull, null)
      }.toList.asJava
      HoodieSchema.createRecord(recordName, null, null, convertedFields)
    case BooleanType =>
      HoodieSchema.create(HoodieSchemaType.BOOLEAN)
    case IntegerType | ShortType | ByteType =>
      HoodieSchema.create(HoodieSchemaType.INT)
    case LongType =>
      HoodieSchema.create(HoodieSchemaType.LONG)
    case FloatType =>
      HoodieSchema.create(HoodieSchemaType.FLOAT)
    case DoubleType =>
      HoodieSchema.create(HoodieSchemaType.DOUBLE)
    case StringType | (_: CharType) | (_: VarcharType) =>
      HoodieSchema.create(HoodieSchemaType.STRING)
    case DateType =>
      HoodieSchema.createDate()
    case TimestampType =>
      HoodieSchema.createTimestampMicros()
    case _type: DecimalType =>
      HoodieSchema.createDecimal(_type.precision, _type.scale)
    case _ =>
      throw new UnsupportedOperationException(s"Cannot convert spark type $sparkType to the relate HUDI type")
  }
}
