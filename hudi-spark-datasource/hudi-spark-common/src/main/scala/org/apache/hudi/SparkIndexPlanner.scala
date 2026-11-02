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

package org.apache.hudi

import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.expression.{Expression => HudiExpression, Predicates}
import org.apache.hudi.common.expression.translate.{ExpressionMappers, ExpressionTranslator}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.mapping.TypeSystems
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.keygen.KeyGenUtils
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.metadata.{HoodieTableMetadata, MetadataPartitionType}
import org.apache.hudi.metadata.planner.{CandidateFiles, DefaultIndexPlanner, HudiStatsEvaluator, IndexLookupContext, IndexPlannerComparison, RecordIndexLookup}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter

import java.util.{EnumSet => JEnumSet, LinkedHashMap => JLinkedHashMap, List => JList}

import scala.collection.JavaConverters._

/**
 * The engine-neutral index planner of RFC-88 (section 4.3) as [[HoodieFileIndex]] uses it when
 * `hoodie.metadata.index.planner.mode` is COMPARE or ON: Catalyst filters become data source [[Filter]]s through
 * Spark's own translation, then a Hudi expression through the `spark` translator; the lookups of a
 * [[DefaultIndexPlanner]] then prune the candidate files the file index gives them.
 *
 * Only created in COMPARE and ON modes.
 *
 * @param metaClient    the table
 * @param tableSchema   the table schema, which types the literals and the column stats
 * @param metadataTable the metadata table the file index's index supports read
 * @param engineContext the engine context the index supports read the metadata table with
 */
private[hudi] class SparkIndexPlanner(metaClient: HoodieTableMetaClient,
                                      tableSchema: HoodieSchema,
                                      metadataTable: HoodieTableMetadata,
                                      engineContext: HoodieEngineContext) extends SparkAdapterSupport with Logging {

  // the record index lookup resolves the key encoding of a single-field complex key as Spark's RecordLevelIndexSupport does
  private val planner = new DefaultIndexPlanner(new RecordIndexLookup(
    new java.util.function.Function[HoodieTableMetaClient, HOption[ComplexKeyGenEncoding]] {
      override def apply(client: HoodieTableMetaClient): HOption[ComplexKeyGenEncoding] =
        KeyGenUtils.resolveComplexKeyGenEncoding(client)
    }))

  private lazy val translator: ExpressionTranslator[Filter] = ExpressionMappers.translator[Filter](TypeSystems.SPARK)

  /**
   * The conjunction of the filters as a Hudi expression: each filter that Spark translates to a data source filter
   * and the `spark` translator translates to a Hudi expression; the others are left out, which can only keep more
   * files. Empty when none translates.
   */
  def toHudiFilter(filters: Seq[Expression]): Option[HudiExpression] = {
    filters
      .flatMap(filter => sparkAdapter.translateFilter(filter))
      .flatMap(filter => toScalaOption(translator.toHudi(filter, tableSchema)))
      .reduceOption((left, right) => Predicates.and(left, right))
  }

  /**
   * Runs the lookup the planner plans for one index.
   *
   * @param index            the index whose lookup runs
   * @param filter           the query filter
   * @param available        the indexes the file index would use for the query
   * @param filesByPartition the candidate file names by relative partition path, as the index supports see them
   * @return the candidate files; [[CandidateFiles.all]] when the index is not available, is not planned or cannot prune
   */
  def lookup(index: MetadataPartitionType,
             filter: HudiExpression,
             available: Set[MetadataPartitionType],
             filesByPartition: Seq[(String, Seq[String])]): CandidateFiles = {
    val availableSet = JEnumSet.noneOf(classOf[MetadataPartitionType])
    available.foreach(availableSet.add)
    planner.plan(filter, tableSchema, availableSet).asScala.find(_.index() == index) match {
      case Some(lookup) =>
        val context = newContext(filesByPartition)
        if (lookup.canPrune(filter, context)) lookup.lookup(filter, context) else CandidateFiles.all()
      case None => CandidateFiles.all()
    }
  }

  /**
   * Prunes partitions with PARTITION_STATS.
   *
   * The lookup prunes whole partitions: it keeps every file of a partition that might match or has no stats. So each
   * partition is given one stand-in file name, which spares listing the files of partitions it prunes, as Spark's
   * partition pruning does.
   *
   * @param partitionPaths every candidate relative partition path
   * @return the partitions kept, or empty if the lookup does not prune
   */
  def prunePartitions(filter: HudiExpression, partitionPaths: Seq[String]): Option[Set[String]] = {
    val standIns = partitionPaths.map(path => path -> (SparkIndexPlanner.PartitionStandIn + path))
    val result = lookup(MetadataPartitionType.PARTITION_STATS, filter, Set(MetadataPartitionType.PARTITION_STATS),
      standIns.map { case (path, standIn) => path -> Seq(standIn) })
    if (result.isAll) {
      None
    } else {
      Some(standIns.collect { case (path, standIn) if result.fileNames.contains(standIn) => path }.toSet)
    }
  }

  private def newContext(filesByPartition: Seq[(String, Seq[String])]): IndexLookupContext = {
    val files = new JLinkedHashMap[String, JList[String]]()
    filesByPartition.foreach { case (partition, names) => files.put(partition, names.asJava) }
    new IndexLookupContext(metaClient, metadataTable, engineContext, tableSchema, HudiStatsEvaluator.getInstance(), files)
  }
}

private[hudi] object SparkIndexPlanner {

  private val PartitionStandIn = "partition:"

  /**
   * Compares the engine's and the planner's results over the same candidates, and logs the differences.
   *
   * @param description the query and table, for the log line
   * @param all         the candidates both start from
   * @param engine      the candidates the engine keeps; empty if it does not prune
   * @param planner     the candidates the planner keeps; empty if it does not prune
   */
  def compare(description: String, all: Seq[String], engine: Option[Set[String]], planner: Option[Set[String]]): IndexPlannerComparison = {
    val comparison = IndexPlannerComparison.compare(all.asJava, toJavaOption(engine.map(_.asJava)),
      planner.map(files => CandidateFiles.of(files.asJava)).getOrElse(CandidateFiles.all()))
    comparison.log(description)
    comparison
  }
}
