/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command

import org.apache.hudi.{DataSourceUtils, HoodieSchemaConversionUtils, HoodieWriterUtils}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, WriteOperationType}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType, HoodieSchemaUtils}
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.schema.internal.action.TableChange.ColumnChangeID
import org.apache.hudi.common.schema.internal.action.TableChanges
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager
import org.apache.hudi.common.schema.internal.utils.{SchemaChangeUtils, SerDeHelper}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.util.NarrowIntAndCharTypes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn, RemoveProperty, SetProperty}
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, MapType, Metadata, MetadataBuilder, ShortType, StructType}

import java.net.URI
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

case class AlterTableCommand(table: CatalogTable, changes: Seq[TableChange], changeType: ColumnChangeID) extends HoodieLeafRunnableCommand with Logging {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    changeType match {
      case ColumnChangeID.ADD => applyAddAction(sparkSession)
      case ColumnChangeID.DELETE => applyDeleteAction(sparkSession)
      case ColumnChangeID.UPDATE => applyUpdateAction(sparkSession)
      case ColumnChangeID.PROPERTY_CHANGE if (changes.filter(_.isInstanceOf[SetProperty]).size == changes.size) =>
        applyPropertySet(sparkSession)
      case ColumnChangeID.PROPERTY_CHANGE if (changes.filter(_.isInstanceOf[RemoveProperty]).size == changes.size) =>
        applyPropertyUnset(sparkSession)
      case ColumnChangeID.REPLACE => applyReplaceAction(sparkSession)
      case other => throw new RuntimeException(s"find unsupported alter command type: ${other}")
    }
    Seq.empty[Row]
  }

  def applyReplaceAction(sparkSession: SparkSession): Unit = {
    // convert to delete first then add again
    val deleteChanges = changes.filter(p => p.isInstanceOf[DeleteColumn]).map(_.asInstanceOf[DeleteColumn])
    val addChanges = changes.filter(p => p.isInstanceOf[AddColumn]).map(_.asInstanceOf[AddColumn])
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, applyDeleteAction2Schema(sparkSession, oldSchema, deleteChanges), addChanges)
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession, Some(oldSchema), addedColumnTypes)
    logInfo("column replace finished")
  }

  def applyAddAction2Schema(sparkSession: SparkSession, oldSchema: InternalSchema, addChanges: Seq[AddColumn]): InternalSchema = {
    val addChange = TableChanges.ColumnAddChange.get(oldSchema)
    addChanges.foreach { addColumn =>
      val names = addColumn.fieldNames()
      val parentName = AlterTableCommand.getParentName(names)
      // add col change
      val colType = SparkInternalSchemaConverter.buildTypeFromStructType(addColumn.dataType(), true, new AtomicInteger(0))
      addChange.addColumns(parentName, names.last, colType, addColumn.comment())
      // add position change
      addColumn.position() match {
        case after: TableChange.After =>
          addChange.addPositionChange(names.mkString("."),
            if (parentName.isEmpty) after.column() else parentName + "." + after.column(), "after")
        case _: TableChange.First =>
          addChange.addPositionChange(names.mkString("."), "", "first")
        case _ =>
      }
    }
    SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange)
  }

  private def applyDeleteAction2Schema(sparkSession: SparkSession, oldSchema: InternalSchema, deleteChanges: Seq[DeleteColumn]): InternalSchema = {
    val deleteChange = TableChanges.ColumnDeleteChange.get(oldSchema)
    deleteChanges.foreach { c =>
      val originalColName = c.fieldNames().mkString(".")
      checkSchemaChange(Seq(originalColName), table)
      deleteChange.deleteColumn(originalColName)
    }
    val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange)
    // delete action should not change the getMaxColumnId field
    newSchema.setMaxColumnId(oldSchema.getMaxColumnId)
    newSchema
  }


  def applyAddAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[AddColumn]))
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession, Some(oldSchema), addedColumnTypes)
    logInfo("column add finished")
  }

  def applyDeleteAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyDeleteAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[DeleteColumn]))
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession, Some(oldSchema), Map.empty)
    logInfo("column delete finished")
  }

  def applyUpdateAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val updateChange = TableChanges.ColumnUpdateChange.get(oldSchema)
    changes.foreach { change =>
      change match {
        case updateType: TableChange.UpdateColumnType =>
          val newType = SparkInternalSchemaConverter.buildTypeFromStructType(updateType.newDataType(), true, new AtomicInteger(0))
          updateChange.updateColumnType(updateType.fieldNames().mkString("."), newType)
        case updateComment: TableChange.UpdateColumnComment =>
          updateChange.updateColumnComment(updateComment.fieldNames().mkString("."), updateComment.newComment())
        case updateName: TableChange.RenameColumn =>
          val originalColName = updateName.fieldNames().mkString(".")
          checkSchemaChange(Seq(originalColName), table)
          updateChange.renameColumn(originalColName, updateName.newName())
        case updateNullAbility: TableChange.UpdateColumnNullability =>
          updateChange.updateColumnNullability(updateNullAbility.fieldNames().mkString("."), updateNullAbility.nullable())
        case updatePosition: TableChange.UpdateColumnPosition =>
          val names = updatePosition.fieldNames()
          val parentName = AlterTableCommand.getParentName(names)
          updatePosition.position() match {
            case after: TableChange.After =>
              updateChange.addPositionChange(names.mkString("."),
                if (parentName.isEmpty) after.column() else parentName + "." + after.column(), "after")
            case _: TableChange.First =>
              updateChange.addPositionChange(names.mkString("."), "", "first")
            case _ =>
          }
      }
    }
    val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, updateChange)
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession, Some(oldSchema), updatedColumnTypes)
    logInfo("column update finished")
  }

  // to do support unset default value to columns, and apply them to internalSchema
  def applyPropertyUnset(sparkSession: SparkSession): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val propKeys = changes.map(_.asInstanceOf[RemoveProperty]).map(_.property())
    // ignore NonExist unset
    propKeys.foreach { k =>
      if (!table.properties.contains(k) && k != TableCatalog.PROP_COMMENT) {
        logWarning(s"Cannot remove property [$k] because it is not currently set for the table.")
      }
    }
    val tableComment = if (propKeys.contains(TableCatalog.PROP_COMMENT)) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    logInfo("table properties change finished")
  }

  // to do support set default value to columns, and apply them to internalSchema
  def applyPropertySet(sparkSession: SparkSession): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val properties = changes.map(_.asInstanceOf[SetProperty]).map(f => f.property -> f.value).toMap
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get(TableCatalog.PROP_COMMENT).orElse(table.comment))
    catalog.alterTable(newTable)
    logInfo("table properties change finished")
  }

  /**
   * The Spark types of the columns this command adds, by full name.
   */
  private def addedColumnTypes: Map[String, DataType] =
    changes.collect { case add: AddColumn => add.fieldNames().mkString(".") -> add.dataType() }.toMap

  /**
   * The new Spark types of the columns this command changes the type of, by full name.
   */
  private def updatedColumnTypes: Map[String, DataType] =
    changes.collect { case update: TableChange.UpdateColumnType => update.fieldNames().mkString(".") -> update.newDataType() }.toMap

  def getInternalSchemaAndHistorySchemaStr(sparkSession: SparkSession): (InternalSchema, String) = {
    val path = AlterTableCommand.getTableLocation(table, sparkSession)
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf())).build()
    val schemaUtil = new TableSchemaResolver(metaClient)

    val schema = schemaUtil.getTableInternalSchemaFromCommitMetadata().orElse {
      InternalSchemaConverter.convert(schemaUtil.getTableSchema)
    }

    val historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata.orElse("")
    (schema, historySchemaStr)
  }

  def checkSchemaChange(colNames: Seq[String], catalogTable: CatalogTable): Unit = {
    val primaryKeys = catalogTable.storage.properties.getOrElse("primaryKey", catalogTable.properties.getOrElse("primaryKey", "keyid")).split(",").map(_.trim)
    val orderingFields = Seq(catalogTable.storage.properties.getOrElse("orderingFields", catalogTable.properties.getOrElse("orderingFields", "ts"))).map(_.trim)
    val partitionKey = catalogTable.partitionColumnNames.map(_.trim)
    val checkNames = primaryKeys ++ orderingFields ++ partitionKey
    colNames.foreach { col =>
      if (checkNames.contains(col)) {
        throw new UnsupportedOperationException("cannot support apply changes for primaryKey/orderingFields/partitionKey")
      }
    }
  }
}

object AlterTableCommand extends Logging {

  /**
    * Generate an commit with new schema to change the table's schema.
    *
    * @param internalSchema new schema after change
    * @param historySchemaStr history schemas
    * @param table The hoodie table.
    * @param sparkSession The spark session.
    */
  def commitWithSchema(internalSchema: InternalSchema, historySchemaStr: String, table: CatalogTable, sparkSession: SparkSession): Unit =
    commitWithSchema(internalSchema, historySchemaStr, table, sparkSession, None, Map.empty)

  /**
   * Generates a commit with the new schema to change the table's schema.
   *
   * InternalSchema has no INT8, INT16 or CHAR(n). For a table that uses them
   * ([[HoodieSchemaConversionUtils.usesNarrowIntAndCharTypes]]), the committed schema takes them back: an existing
   * column (renamed or not) from the table's schema before the change, through the column ids of `oldInternalSchema`,
   * and an added or retyped column from its Spark type in `changedColumnTypes`, by the table's mapping.
   *
   * @param oldInternalSchema  the schema before the change
   * @param changedColumnTypes the Spark types of the columns the change adds or retypes, by full name
   */
  def commitWithSchema(internalSchema: InternalSchema, historySchemaStr: String, table: CatalogTable, sparkSession: SparkSession,
                       oldInternalSchema: scala.Option[InternalSchema], changedColumnTypes: Map[String, DataType]): Unit = {
    val path = getTableLocation(table, sparkSession)
    val tableMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf()))
      .build()
    val narrowIntAndCharTypes = HoodieSchemaConversionUtils.usesNarrowIntAndCharTypes(tableMetaClient.getTableConfig)
    val convertedSchema = InternalSchemaConverter.convert(internalSchema, HoodieSchemaUtils.getRecordQualifiedName(table.identifier.table))
    val schema = if (narrowIntAndCharTypes) {
      restoreNarrowTypes(convertedSchema, internalSchema, oldInternalSchema, changedColumnTypes, tableMetaClient)
    } else {
      convertedSchema
    }
    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(
      jsc,
      schema.toString,
      path,
      table.identifier.table,
      HoodieWriterUtils.parametersWithWriteDefaults(
        HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(table.storage.properties ++ table.properties) ++
        sparkSession.sessionState.conf.getAllConfs ++ Map(
        HoodieCleanConfig.AUTO_CLEAN.key -> "false",
        HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.NEVER.name,
        HoodieArchivalConfig.AUTO_ARCHIVE.key -> "false"
        )).asJava)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf()))
      .setTimeGeneratorConfig(client.getConfig.getTimeGeneratorConfig)
      .build()

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = client.startCommit(commitActionType)
    client.setOperationType(WriteOperationType.ALTER_SCHEMA)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    hoodieTable.validateSchema()
    val timeLine = hoodieTable.getActiveTimeline
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val requested = instantGenerator.createNewInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata))
    val extraMeta = new util.HashMap[String, String]()
    extraMeta.put(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema.setSchemaId(instantTime.toLong)))
    val schemaManager = new FileBasedInternalSchemaStorageManager(metaClient)
    schemaManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(internalSchema, historySchemaStr))
    client.commit(instantTime, jsc.emptyRDD, Option.of(extraMeta))
    val existRoTable = sparkSession.catalog.tableExists(table.identifier.unquotedString + "_ro")
    val existRtTable = sparkSession.catalog.tableExists(table.identifier.unquotedString + "_rt")
    try {
      sparkSession.catalog.refreshTable(table.identifier.unquotedString)
      // try to refresh ro/rt table
      if (existRoTable) sparkSession.catalog.refreshTable(table.identifier.unquotedString + "_ro")
      if (existRoTable) sparkSession.catalog.refreshTable(table.identifier.unquotedString + "_rt")
    } catch {
      case NonFatal(e) =>
        log.error(s"Exception when attempting to refresh table ${table.identifier.quotedString}", e)
    }
    // try to sync to hive
    // drop partition field before call alter table
    val internalSparkSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(internalSchema)
    val fullSparkSchema = if (narrowIntAndCharTypes) {
      withNarrowTypes(internalSparkSchema, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema))
    } else {
      internalSparkSchema
    }
    val dataSparkSchema = new StructType(fullSparkSchema.fields.filter(p => !table.partitionColumnNames.exists(f => sparkSession.sessionState.conf.resolver(f, p.name))))
    alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table, dataSparkSchema)
    if (existRoTable) alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table + "_ro", dataSparkSchema)
    if (existRtTable) alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table + "_rt", dataSparkSchema)
  }

  private val NARROW_TYPES = java.util.EnumSet.of(HoodieSchemaType.INT8, HoodieSchemaType.INT16, HoodieSchemaType.CHAR)

  /**
   * The catalog schema built from the InternalSchema (which keeps the column comments), with the types of the columns
   * the table schema has as INT8, INT16 or CHAR(n): tinyint, smallint and char(n), with their `hudi_type` and Spark
   * char metadata.
   */
  private def withNarrowTypes(internalSparkSchema: StructType, tableSparkSchema: StructType): StructType = {
    StructType(internalSparkSchema.fields.map { field =>
      tableSparkSchema.fields.find(_.name == field.name) match {
        case Some(typed) if hasNarrowInt(typed.dataType) || hasNarrowTypeMetadata(typed.metadata) =>
          val metadata = new MetadataBuilder().withMetadata(field.metadata)
          Seq(HoodieSchema.TYPE_METADATA_FIELD, CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)
            .filter(typed.metadata.contains)
            .foreach(key => metadata.putString(key, typed.metadata.getString(key)))
          field.copy(dataType = typed.dataType, metadata = metadata.build())
        case _ => field
      }
    })
  }

  private def hasNarrowInt(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType => true
    case ArrayType(elementType, _) => hasNarrowInt(elementType)
    case MapType(_, valueType, _) => hasNarrowInt(valueType)
    case struct: StructType => struct.fields.exists(f => hasNarrowInt(f.dataType))
    case _ => false
  }

  private def hasNarrowTypeMetadata(metadata: Metadata): Boolean =
    metadata.contains(CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY) ||
      Seq(HoodieSchemaType.INT8, HoodieSchemaType.INT16, HoodieSchemaType.CHAR)
        .exists(t => HoodieSparkSchemaConverters.hasTypeMarker(metadata, t))

  /**
   * Takes INT8, INT16 and CHAR(n) back into a schema converted from the InternalSchema after a change, wherever it has
   * the type they become there (INT, STRING): for an added or retyped column from its Spark type, otherwise from the
   * table's schema before the change at the column's name before the change (InternalSchema keeps column ids across
   * renames). Columns are named as InternalSchema names them (`a.b`, `arr.element`, `m.value`).
   */
  private def restoreNarrowTypes(converted: HoodieSchema,
                                 internalSchema: InternalSchema,
                                 oldInternalSchema: scala.Option[InternalSchema],
                                 changedColumnTypes: Map[String, DataType],
                                 metaClient: HoodieTableMetaClient): HoodieSchema = {
    lazy val oldTableSchema = Try(new TableSchemaResolver(metaClient).getTableSchema(false)).toOption
    val changedSchemas = changedColumnTypes.map { case (name, dataType) =>
      name -> HoodieSparkSchemaConverters.toHoodieType(dataType, nullable = false, "column", "", Metadata.empty,
        narrowIntAndCharTypes = true)
    }

    def lookup(name: String): scala.Option[HoodieSchema] = {
      changedSchemas.find { case (changed, _) => name == changed || name.startsWith(changed + ".") } match {
        case Some((changed, changedSchema)) =>
          walk(changedSchema, name.substring(changed.length).split('.').filter(_.nonEmpty).toList)
        case None =>
          val id = internalSchema.findIdByName(name)
          val oldName = if (id < 0) "" else oldInternalSchema.map(_.findFullName(id)).getOrElse(name)
          if (oldName.isEmpty) None else oldTableSchema.flatMap(walk(_, oldName.split('.').toList))
      }
    }

    def restore(schema: HoodieSchema, name: String): HoodieSchema = {
      val nonNull = if (schema.isNullable && !schema.isComplexUnion) schema.getNonNullType else schema
      val restored = nonNull.getType match {
        case HoodieSchemaType.RECORD =>
          val fields = nonNull.getFields.asScala.map(f => f -> restore(f.schema(), if (name.isEmpty) f.name() else s"$name.${f.name()}"))
          if (fields.forall { case (f, s) => s eq f.schema() }) {
            nonNull
          } else {
            HoodieSchema.createRecord(nonNull.getName, nonNull.getNamespace.orElse(null), nonNull.getDoc.orElse(null),
              fields.map { case (f, s) => f.withSchema(s) }.asJava)
          }
        case HoodieSchemaType.ARRAY =>
          val element = restore(nonNull.getElementType, s"$name.element")
          if (element eq nonNull.getElementType) nonNull else HoodieSchema.createArray(element)
        case HoodieSchemaType.MAP =>
          val value = restore(nonNull.getValueType, s"$name.value")
          if (value eq nonNull.getValueType) nonNull else HoodieSchema.createMap(value)
        case HoodieSchemaType.INT | HoodieSchemaType.STRING =>
          lookup(name).filter(n => NARROW_TYPES.contains(n.getType) && NarrowIntAndCharTypes.widened(n).equals(nonNull))
            .getOrElse(nonNull)
        case _ => nonNull
      }
      if (restored eq nonNull) schema else if (schema.isNullable && !schema.isComplexUnion) HoodieSchema.createNullable(restored) else restored
    }

    restore(converted, "")
  }

  /**
   * The node at an InternalSchema-style name (`a.b`, `arr.element`, `m.value`) in a schema.
   */
  private def walk(schema: HoodieSchema, names: List[String]): scala.Option[HoodieSchema] = {
    val nonNull = if (schema.isNullable && !schema.isComplexUnion) schema.getNonNullType else schema
    names match {
      case Nil => Some(nonNull)
      case head :: tail => nonNull.getType match {
        case HoodieSchemaType.RECORD =>
          val field = nonNull.getField(head)
          if (field.isPresent) walk(field.get().schema(), tail) else None
        case HoodieSchemaType.ARRAY if head == "element" => walk(nonNull.getElementType, tail)
        case HoodieSchemaType.MAP if head == "value" => walk(nonNull.getValueType, tail)
        case _ => None
      }
    }
  }

  def alterTableDataSchema(sparkSession: SparkSession, db: String, tableName: String, dataSparkSchema: StructType): Unit = {
    sparkSession.sessionState.catalog
      .externalCatalog
      .alterTableDataSchema(db, tableName, dataSparkSchema)
  }

  def getTableLocation(table: CatalogTable, sparkSession: SparkSession): String = {
    val uri = if (table.tableType == CatalogTableType.MANAGED) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
      .getOrElse(throw new IllegalArgumentException(s"Missing location for ${table.identifier}"))
  }

  private def removePlaceHolder(path: String): String = {
    if (path == null || path.length == 0) {
      path
    } else if (path.endsWith("-PLACEHOLDER")) {
      path.substring(0, path.length() - 16)
    } else {
      path
    }
  }

  def makePathQualified(path: URI, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri.toString
  }

  def getParentName(names: Array[String]): String = {
    if (names.size > 1) {
      names.dropRight(1).mkString(".")
    } else ""
  }
}
