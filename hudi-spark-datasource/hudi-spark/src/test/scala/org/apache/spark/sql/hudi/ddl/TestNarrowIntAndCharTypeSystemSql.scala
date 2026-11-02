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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.hive.util.HiveSchemaUtil
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * RFC-88 step 2c, compatibility tests for Spark SQL: CREATE TABLE, CTAS, INSERT, MERGE INTO and ALTER TABLE ADD
 * COLUMNS with TINYINT, SMALLINT, CHAR(n) and VARCHAR(n) columns. A new table gets INT8, INT16 and CHAR(n) in its
 * table schema and reads back as tinyint, smallint and char(n); a table created before
 * `hoodie.table.type.system.version` existed keeps INT and STRING and reads back as before.
 */
class TestNarrowIntAndCharTypeSystemSql extends HoodieSparkSqlTestBase {

  private val INT = HoodieSchema.create(HoodieSchemaType.INT)
  private val STRING = HoodieSchema.create(HoodieSchemaType.STRING)

  test("Test new table: CREATE TABLE, INSERT, MERGE INTO and ALTER TABLE ADD COLUMNS give INT8, INT16 and CHAR(n)") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType => withClue(s"$tableType table: ") {
        val tableName = generateTableName
        val path = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""create table $tableName (
             |  id int, tiny tinyint, small smallint, code char(10), name varchar(8), ts long
             |) using hudi
             |tblproperties (type = '$tableType', primaryKey = 'id', orderingFields = 'ts')
             |location '$path'""".stripMargin)
        val metaClient = createMetaClient(spark, path)
        assertResult("1")(metaClient.getTableConfig.getProps.getProperty(HoodieTableConfig.TYPE_SYSTEM_VERSION.key))
        // the create schema, before any commit
        assertColumns(tableSchema(path), "tiny" -> HoodieSchema.createInt8(), "small" -> HoodieSchema.createInt16(),
          "code" -> HoodieSchema.createChar(10), "name" -> STRING)

        writeRows(tableName, narrowSource = true)
        spark.sql(s"alter table $tableName add columns (extra tinyint, extra_code char(3))")
        spark.sql(s"insert into $tableName values (4, 1, 1, 'k4', 'n4', 1, -1, 'z')")

        val schema = tableSchema(path)
        assertColumns(schema, "tiny" -> HoodieSchema.createInt8(), "small" -> HoodieSchema.createInt16(),
          "code" -> HoodieSchema.createChar(10), "name" -> STRING, "extra" -> HoodieSchema.createInt8(),
          "extra_code" -> HoodieSchema.createChar(3))
        assertResult(Seq("tinyint", "smallint", "char(10)", "string", "tinyint", "char(3)"))(hiveTypes(schema))
        val read = spark.sql(s"select id, tiny, small, code, name, extra, extra_code from $tableName order by id")
        assertResult(Seq("int", "tinyint", "smallint", "char(10)", "string", "tinyint", "char(3)"))(sparkTypes(read.schema))
        assertRows(read, padChar = true, narrow = true)
        checkAnswer(s"select id from $tableName where code = 'abc' and tiny = 127 and small = -32768")(Seq(2))
      }}
    }
  }

  test("Test new table: CTAS gives INT8, INT16 and CHAR(n)") {
    withTempDir { tmp =>
      val source = generateTableName
      spark.sql(s"create table $source (id int, tiny tinyint, small smallint, code char(5), ts long) using parquet")
      spark.sql(s"insert into $source values (1, -128, 32767, 'ab', 1), (2, 127, -32768, 'abcde', 1)")
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""create table $tableName using hudi
           |tblproperties (primaryKey = 'id', orderingFields = 'ts')
           |location '$path'
           |as select * from $source""".stripMargin)
      assertColumns(tableSchema(path), "tiny" -> HoodieSchema.createInt8(), "small" -> HoodieSchema.createInt16(),
        "code" -> HoodieSchema.createChar(5))
      val read = spark.sql(s"select id, tiny, small, code from $tableName order by id")
      assertResult(Seq("int", "tinyint", "smallint", "char(5)"))(sparkTypes(read.schema))
      assertResult(Seq(Row(1, -128.toByte, 32767.toShort, "ab   "), Row(2, 127.toByte, -32768.toShort, "abcde")))(
        read.collect().toSeq)
    }
  }

  test("Test existing table: INSERT, MERGE INTO and ALTER TABLE ADD COLUMNS keep INT and STRING") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType => withClue(s"$tableType table: ") {
        val tableName = generateTableName
        val path = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""create table $tableName (id int, tiny int, small int, code string, name string, ts long) using hudi
             |tblproperties (type = '$tableType', primaryKey = 'id', orderingFields = 'ts')
             |location '$path'""".stripMargin)
        // a table as master creates it: no type system version in hoodie.properties
        val metaClient = createMetaClient(spark, path)
        HoodieTableConfig.delete(metaClient.getStorage, metaClient.getMetaPath,
          Set(HoodieTableConfig.TYPE_SYSTEM_VERSION.key).asJava)
        spark.sql(s"refresh table $tableName")

        writeRows(tableName, narrowSource = false)
        // columns added later keep today's mapping as well
        spark.sql(s"alter table $tableName add columns (extra tinyint, extra_code char(3))")
        spark.sql(s"insert into $tableName values (4, 1, 1, 'k4', 'n4', 1, -1, 'z')")

        assertResult(false)(createMetaClient(spark, path).getTableConfig.getProps.containsKey(
          HoodieTableConfig.TYPE_SYSTEM_VERSION.key))
        val schema = tableSchema(path)
        assertColumns(schema, "tiny" -> INT, "small" -> INT, "code" -> STRING, "name" -> STRING, "extra" -> INT,
          "extra_code" -> STRING)
        assertResult(Seq("int", "int", "string", "string", "int", "string"))(hiveTypes(schema))
        val read = spark.sql(s"select id, tiny, small, code, name, extra, extra_code from $tableName order by id")
        assertResult(Seq("int", "int", "int", "string", "string", "int", "string"))(sparkTypes(read.schema))
        assertRows(read, padChar = false, narrow = false)
      }}
    }
  }

  test("Test schema on read: ALTER TABLE ADD, RENAME and ALTER COLUMN TYPE through InternalSchema") {
    withTempDir { tmp =>
      Seq(("cow", true), ("mor", true), ("cow", false), ("mor", false)).foreach { case (tableType, newTable) =>
        withClue(s"$tableType ${if (newTable) "new" else "existing"} table: ") {
          withSQLConf("hoodie.schema.on.read.enable" -> "true", "hoodie.metadata.index.column.stats.enable" -> "false",
            "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true") {
            val tableName = generateTableName
            val path = s"${tmp.getCanonicalPath}/$tableName"
            val (tinyType, smallType, codeType) = if (newTable) ("tinyint", "smallint", "char(10)") else ("int", "int", "string")
            spark.sql(
              s"""create table $tableName (id int, tiny $tinyType, small $smallType, code $codeType, name string, ts long)
                 |using hudi
                 |tblproperties (type = '$tableType', primaryKey = 'id', orderingFields = 'ts')
                 |location '$path'""".stripMargin)
            if (!newTable) {
              val metaClient = createMetaClient(spark, path)
              HoodieTableConfig.delete(metaClient.getStorage, metaClient.getMetaPath,
                Set(HoodieTableConfig.TYPE_SYSTEM_VERSION.key).asJava)
              spark.sql(s"refresh table $tableName")
            }
            spark.sql(s"insert into $tableName values (1, -128, 32767, 'abc', 'n1', 1), (2, 127, -32768, 'x', 'n2', 1)")
            spark.sql(s"alter table $tableName add columns (extra tinyint, extra_code char(3))")
            spark.sql(s"alter table $tableName rename column name to name2")
            spark.sql(s"alter table $tableName alter column small type int")
            spark.sql(s"insert into $tableName values (3, 1, 100000, 'k3', 'n3', 1, -1, 'z')")

            val metaClient = createMetaClient(spark, path)
            assert(new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata.isPresent)
            val schema = tableSchema(path)
            if (newTable) {
              // INT16 -> INT is a type widening, the others keep their types through every InternalSchema change
              assertColumns(schema, "tiny" -> HoodieSchema.createInt8(), "small" -> INT, "code" -> HoodieSchema.createChar(10),
                "name2" -> STRING, "extra" -> HoodieSchema.createInt8(), "extra_code" -> HoodieSchema.createChar(3))
            } else {
              assertColumns(schema, "tiny" -> INT, "small" -> INT, "code" -> STRING, "name2" -> STRING, "extra" -> INT,
                "extra_code" -> STRING)
            }
            val read = spark.sql(s"select id, tiny, small, code, name2, extra, extra_code from $tableName order by id")
            if (newTable) {
              assertResult(Seq("int", "tinyint", "int", "char(10)", "string", "tinyint", "char(3)"))(sparkTypes(read.schema))
            } else {
              assertResult(Seq("int", "int", "int", "string", "string", "int", "string"))(sparkTypes(read.schema))
            }
            def code(v: String, length: Int = 10): String = if (newTable) v.padTo(length, ' ') else v
            def tiny(v: Int): Any = if (newTable) v.toByte else v
            assertResult(Seq(
              Row(1, tiny(-128), 32767, code("abc"), "n1", null, null),
              Row(2, tiny(127), -32768, code("x"), "n2", null, null),
              Row(3, tiny(1), 100000, code("k3"), "n3", tiny(-1), code("z", 3))))(read.collect().toSeq)
          }
        }
      }
    }
  }

  /**
   * Inserts the edge values, then updates and inserts through MERGE INTO, with TINYINT and SMALLINT source columns
   * when `narrowSource` (the column types of the new table) and INT ones otherwise (the column types an existing
   * table shows; a MERGE INTO assigning a TINYINT or SMALLINT source column to an INT column of an existing table fails
   * in ExpressionPayload with a ClassCastException, MutableInt to MutableByte, which this change does not touch).
   */
  private def writeRows(tableName: String, narrowSource: Boolean): Unit = {
    val (tinyType, smallType) = if (narrowSource) ("tinyint", "smallint") else ("int", "int")
    spark.sql(
      s"""insert into $tableName values
         |(1, -128, 32767, 'abcdefghij', 'n1', 1),
         |(2, 127, -32768, 'abc', 'n2', 1),
         |(3, 0, null, 'x', null, 1)""".stripMargin)
    spark.sql(
      s"""merge into $tableName t
         |using (
         |  select 1 as id, cast(5 as $tinyType) as tiny, cast(-5 as $smallType) as small, '0123456789' as code,
         |    'upd' as name, cast(2 as long) as ts
         |  union all
         |  select 5 as id, cast(-1 as $tinyType) as tiny, cast(-1 as $smallType) as small, 'm' as code,
         |    'n5' as name, cast(1 as long) as ts
         |) s
         |on t.id = s.id
         |when matched then update set *
         |when not matched then insert *
         |""".stripMargin)
  }

  private def assertRows(read: DataFrame, padChar: Boolean, narrow: Boolean): Unit = {
    def code(v: String, length: Int = 10): String = if (v == null || !padChar) v else v.padTo(length, ' ')
    def tiny(v: Int): Any = if (narrow) v.toByte else v
    def small(v: Int): Any = if (narrow) v.toShort else v
    assertResult(Seq(
      Row(1, tiny(5), small(-5), code("0123456789"), "upd", null, null),
      Row(2, tiny(127), small(-32768), code("abc"), "n2", null, null),
      Row(3, tiny(0), null, code("x"), null, null, null),
      Row(4, tiny(1), small(1), code("k4"), "n4", tiny(-1), code("z", 3)),
      Row(5, tiny(-1), small(-1), code("m"), "n5", null, null)))(read.collect().toSeq)
  }

  private def tableSchema(path: String): HoodieSchema = {
    val metaClient: HoodieTableMetaClient = createMetaClient(spark, path)
    new TableSchemaResolver(metaClient).getTableSchema(false)
  }

  private def assertColumns(schema: HoodieSchema, columns: (String, HoodieSchema)*): Unit = {
    columns.foreach { case (name, expected) =>
      val field = schema.getField(name)
      assert(field.isPresent, s"column $name in $schema")
      assertResult(expected, s"column $name in $schema")(field.get().schema().getNonNullType)
    }
  }

  private def hiveTypes(schema: HoodieSchema): Seq[String] = {
    val hive = HiveSchemaUtil.convertSchemaToHiveSchema(schema, false).asScala.map { case (k, v) => k.replace("`", "") -> v }
    Seq("tiny", "small", "code", "name", "extra", "extra_code").map(c => hive(c).toLowerCase)
  }

  private def sparkTypes(schema: StructType): Seq[String] =
    schema.fields.map(f => CharVarcharUtils.getRawType(f.metadata).getOrElse(f.dataType).simpleString).toSeq
}
