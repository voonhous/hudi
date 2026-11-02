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

package org.apache.hudi.source;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.planner.CandidateFiles;
import org.apache.hudi.metadata.planner.DefaultIndexPlanner;
import org.apache.hudi.metadata.planner.HudiStatsEvaluator;
import org.apache.hudi.metadata.planner.IndexLookup;
import org.apache.hudi.metadata.planner.IndexLookupContext;
import org.apache.hudi.metadata.planner.IndexPlannerComparison;
import org.apache.hudi.metadata.planner.IndexPlannerMode;
import org.apache.hudi.metadata.planner.RecordIndexLookup;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.stats.FileStatsIndex;
import org.apache.hudi.source.stats.PartitionStatsIndex;
import org.apache.hudi.util.ExpressionUtils;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wires the engine-neutral index planner of RFC-88 (section 4.3, {@link DefaultIndexPlanner}) into Flink's data
 * skipping, behind {@link HoodieMetadataConfig#INDEX_PLANNER_MODE}.
 *
 * <ul>
 *   <li>{@link IndexPlannerMode#OFF} (default): nothing here is created or called, Flink's data skipping runs as
 *       before;</li>
 *   <li>{@link IndexPlannerMode#COMPARE}: Flink's own pruning runs and its result is used; the planner runs too on the
 *       same candidates and {@link IndexPlannerComparison} logs the differences. A planner failure is logged and
 *       ignored;</li>
 *   <li>{@link IndexPlannerMode#ON}: the planner replaces Flink's pruning with the indexes it covers, at the position
 *       Flink applies them; a planner failure keeps every candidate (no pruning from the planner), as Flink's own
 *       index lookups do on failure.</li>
 * </ul>
 *
 * <p>Flink applies its indexes at two stages, and the planner is wired into both:
 * <ol>
 *   <li>partition pruning ({@link PartitionPruners}): partition stats, after the static and dynamic (partition filter)
 *       pruners, whether or not the query has a partition filter. {@link PartitionStatsPlanner} replaces Flink's
 *       {@code ColumnStatsPartitionPruner} there. As the files are not listed yet, a partition stands for its files:
 *       the candidates of the lookup are the partitions, each with itself as its only "file";</li>
 *   <li>file slice pruning ({@link FileIndex#filterFileSlices}): bucket pruning first (not covered by the planner, it
 *       stays), then the record index and column stats, whose results Flink intersects. {@link FilePlanner} replaces
 *       those two with {@link DefaultIndexPlanner#lookupAll}, the planner's intersection, so that COMPARE compares like
 *       with like. A file slice is read when any of its files is kept, as with Flink's column stats pruning, and the
 *       comparison is made over the files of the kept slices.</li>
 * </ol>
 *
 * <p>The planner only gets the indexes Flink itself would use:
 * <ul>
 *   <li>PARTITION_STATS when Flink builds its partition stats pruner: data filters referencing columns, data skipping
 *       and the metadata table enabled, a partitioned table (the lookup checks that the index is built);</li>
 *   <li>RECORD_INDEX when Flink builds its record index support ({@code BaseRecordLevelIndex#create}): a snapshot
 *       query, record keys in the filters within {@code read.data.skipping.rli.keys.max.num}, a known key encoding
 *       and a record index definition. Like Flink, this does not depend on {@code read.data.skipping.enabled};</li>
 *   <li>COLUMN_STATS when Flink uses column stats: data filters referencing columns, data skipping and the metadata
 *       table enabled (the lookup checks that the index is built).</li>
 * </ul>
 *
 * <p>The filter is the conjunction of the pushed data filters (the partition filters prune partitions as before),
 * translated by {@link FlinkTranslator}; a filter that does not translate is left out, which only keeps more files.
 */
@Slf4j
public final class IndexPlannerSupport {

  private static volatile Consumer<Report> testListener;

  private IndexPlannerSupport() {
  }

  /**
   * The index planner mode set in the Flink configuration ({@code hoodie.metadata.index.planner.mode}).
   */
  public static IndexPlannerMode mode(Configuration conf) {
    String mode = conf.getString(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), HoodieMetadataConfig.INDEX_PLANNER_MODE.defaultValue());
    return IndexPlannerMode.valueOf(mode.trim().toUpperCase(Locale.ROOT));
  }

  /**
   * The Hudi schema the planner types columns and literals with: the table's Flink row type, as Flink's own data
   * skipping decodes the column stats with it.
   */
  public static HoodieSchema schemaOf(RowType rowType) {
    return HoodieSchemaConverter.convertToSchema(rowType);
  }

  /**
   * The conjunction of the filters as a Hudi expression; a filter that does not translate is left out.
   *
   * @return empty if no filter translates
   */
  public static Option<Expression> translate(List<ResolvedExpression> filters, HoodieSchema schema) {
    ExpressionTranslator<ResolvedExpression> translator = ExpressionMappers.translator(TypeSystems.FLINK);
    Expression conjunction = null;
    for (ResolvedExpression filter : filters) {
      Option<Expression> translated;
      try {
        translated = translator.toHudi(filter, schema);
      } catch (RuntimeException e) {
        log.warn("Index planner: cannot translate filter {}, left out", filter.asSummaryString(), e);
        translated = Option.empty();
      }
      if (translated.isPresent()) {
        Expression expr = orOfEqualitiesAsIn(translated.get());
        conjunction = conjunction == null ? expr : Predicates.and(conjunction, expr);
      }
    }
    return Option.ofNullable(conjunction);
  }

  /**
   * A disjunction of equalities on one column as an IN, e.g. {@code a = 1 OR a = 2} as {@code a IN (1, 2)}; any other
   * expression as it is. Flink pushes {@code a IN (1, 2)} down as the disjunction (it expands SEARCH), which Flink's
   * record index support reads ({@code BaseRecordLevelIndex#collectLiterals}) but the planner's
   * {@link RecordIndexLookup} only reads as an IN. Both forms are equivalent.
   */
  @VisibleForTesting
  static Expression orOfEqualitiesAsIn(Expression expr) {
    if (!(expr instanceof Predicates.Or)) {
      return expr;
    }
    List<Expression> disjuncts = new ArrayList<>();
    collectDisjuncts(expr, disjuncts);
    NameReference column = null;
    List<Expression> values = new ArrayList<>();
    for (Expression disjunct : disjuncts) {
      if (!(disjunct instanceof Predicates.BinaryComparison) || ((Predicates.BinaryComparison) disjunct).getOperator() != Expression.Operator.EQ) {
        return expr;
      }
      Predicates.BinaryComparison eq = (Predicates.BinaryComparison) disjunct;
      if (!(eq.getLeft() instanceof NameReference) || !(eq.getRight() instanceof Literal)) {
        return expr;
      }
      NameReference name = (NameReference) eq.getLeft();
      if (column != null && !column.getName().equals(name.getName())) {
        return expr;
      }
      column = name;
      values.add(eq.getRight());
    }
    return Predicates.in(column, values);
  }

  private static void collectDisjuncts(Expression expr, List<Expression> disjuncts) {
    if (expr instanceof Predicates.Or) {
      collectDisjuncts(((Predicates.Or) expr).getLeft(), disjuncts);
      collectDisjuncts(((Predicates.Or) expr).getRight(), disjuncts);
    } else {
      disjuncts.add(expr);
    }
  }

  /**
   * The probe with the Hudi translation of its filters attached, for the planner; the probe as it is if the mode is
   * OFF or nothing translates.
   */
  @Nullable
  public static ColumnStatsProbe withPlannerFilter(@Nullable ColumnStatsProbe probe, List<ResolvedExpression> dataFilters, Configuration conf, RowType rowType) {
    if (probe == null || mode(conf) == IndexPlannerMode.OFF) {
      return probe;
    }
    try {
      HoodieSchema schema = schemaOf(rowType);
      Option<Expression> filter = translate(withoutNarrowIntsStoredAsInt(dataFilters, rowType, schema), schema);
      return filter.isPresent() ? probe.withPlannerFilter(filter.get()) : probe;
    } catch (Throwable t) {
      // the planner then prunes nothing, the query is not affected
      log.warn("Index planner: cannot translate the filters {}, the planner will not prune", dataFilters, t);
      return probe;
    }
  }

  /**
   * The filters without those on TINYINT / SMALLINT columns the schema stores as INT.
   *
   * <p>Files Flink wrote to such columns before it stored them sign extended hold negative values as their unsigned
   * byte pattern (-1 as 255 or 65535), and so do their statistics, while Flink reads the values truncated. The planner
   * compares the statistics as they are, so it does not use filters on these columns (see
   * rfc/rfc-88/data-skipping-issues.md, item 9). Leaving a filter out only keeps more candidates.
   */
  @VisibleForTesting
  static List<ResolvedExpression> withoutNarrowIntsStoredAsInt(List<ResolvedExpression> filters, RowType rowType, HoodieSchema schema) {
    Set<String> narrowIntsStoredAsInt = new HashSet<>();
    for (RowType.RowField field : rowType.getFields()) {
      LogicalTypeRoot root = field.getType().getTypeRoot();
      if (root == LogicalTypeRoot.TINYINT || root == LogicalTypeRoot.SMALLINT) {
        Option<HoodieSchemaField> schemaField = schema.getField(field.getName());
        if (schemaField.isPresent() && schemaField.get().schema().getNonNullType().getType() == HoodieSchemaType.INT) {
          narrowIntsStoredAsInt.add(field.getName());
        }
      }
    }
    if (narrowIntsStoredAsInt.isEmpty()) {
      return filters;
    }
    return filters.stream()
        .filter(filter -> Arrays.stream(ExpressionUtils.referencedColumns(Collections.singletonList(filter))).noneMatch(narrowIntsStoredAsInt::contains))
        .collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  File slice stage
  // -------------------------------------------------------------------------

  /**
   * Replaces (ON) or shadows (COMPARE) the record index and column stats pruning of {@link FileIndex#filterFileSlices}.
   */
  public static final class FilePlanner implements Serializable {
    private static final long serialVersionUID = 1L;

    private final IndexPlannerMode mode;
    private final Configuration conf;
    private final RowType rowType;
    @Nullable
    private final ColumnStatsProbe probe;
    private final boolean recordIndexUsed;
    private final boolean columnStatsUsed;
    private final FileStatsIndex metadataSource;
    @Nullable
    private HoodieTableMetaClient metaClient;

    /**
     * @param recordIndexUsed  whether Flink would use the record index for this query
     * @param columnStatsUsed  whether Flink would use column stats for this query
     * @param metadataSource   the column stats index of the file index, whose metadata table the planner shares
     */
    public FilePlanner(IndexPlannerMode mode, Configuration conf, RowType rowType, @Nullable HoodieTableMetaClient metaClient,
                       @Nullable ColumnStatsProbe probe, boolean recordIndexUsed, boolean columnStatsUsed, FileStatsIndex metadataSource) {
      this.mode = mode;
      this.conf = conf;
      this.rowType = rowType;
      this.metaClient = metaClient;
      this.probe = probe;
      this.recordIndexUsed = recordIndexUsed;
      this.columnStatsUsed = columnStatsUsed;
      this.metadataSource = metadataSource;
    }

    /**
     * @param fileSlices the candidate file slices, after the pruning that comes before the planner's indexes
     * @param engine     Flink's own record index and column stats pruning
     */
    public List<FileSlice> filterFileSlices(List<FileSlice> fileSlices, Function<List<FileSlice>, List<FileSlice>> engine) {
      Set<String> allFiles = filesOf(fileSlices);
      if (mode == IndexPlannerMode.COMPARE) {
        List<FileSlice> engineResult = engine.apply(fileSlices);
        try {
          CandidateFiles planner = keptFiles(fileSlices, lookup(fileSlices));
          IndexPlannerComparison comparison = IndexPlannerComparison.compare(allFiles, Option.of(filesOf(engineResult)), planner);
          comparison.log(describe());
          report(new Report(Stage.FILES, mode, filter(), allFiles, filesOf(engineResult), planner, comparison, null));
        } catch (Throwable t) {
          log.warn("Index planner COMPARE failed for {}, ignored", describe(), t);
          report(new Report(Stage.FILES, mode, filter(), allFiles, filesOf(engineResult), null, null, t));
        }
        return engineResult;
      }
      CandidateFiles candidates;
      Throwable failure = null;
      try {
        candidates = lookup(fileSlices);
      } catch (Throwable t) {
        log.warn("Index planner failed for {}, no pruning from the planner", describe(), t);
        candidates = CandidateFiles.all();
        failure = t;
      }
      List<FileSlice> result = candidates.isAll() ? fileSlices : slicesWithAnyOf(fileSlices, candidates.fileNames());
      log.info("Index planner pruning for {}: {} file slices, {} left", describe(), fileSlices.size(), result.size());
      report(new Report(Stage.FILES, mode, filter(), allFiles, null, keptFiles(fileSlices, candidates), null, failure));
      return result;
    }

    private CandidateFiles lookup(List<FileSlice> fileSlices) {
      Expression filter = filter();
      Set<MetadataPartitionType> available = EnumSet.noneOf(MetadataPartitionType.class);
      if (recordIndexUsed) {
        available.add(MetadataPartitionType.RECORD_INDEX);
      }
      if (columnStatsUsed) {
        available.add(MetadataPartitionType.COLUMN_STATS);
      }
      if (filter == null || available.isEmpty() || fileSlices.isEmpty()) {
        return CandidateFiles.all();
      }
      HoodieSchema schema = schemaOf(rowType);
      List<IndexLookup> plan = newPlanner().plan(filter, schema, available);
      if (plan.isEmpty()) {
        return CandidateFiles.all();
      }
      Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
      for (FileSlice fileSlice : fileSlices) {
        filesByPartition.computeIfAbsent(fileSlice.getPartitionPath(), p -> new ArrayList<>()).addAll(fileSlice.getAllFileNames());
      }
      IndexLookupContext context = new IndexLookupContext(metaClient(), metadataSource.getMetadataTable(), HoodieFlinkEngineContext.DEFAULT,
          schema, HudiStatsEvaluator.getInstance(), filesByPartition);
      return DefaultIndexPlanner.lookupAll(plan, filter, context);
    }

    @Nullable
    private Expression filter() {
      return probe == null ? null : probe.getPlannerFilter();
    }

    private HoodieTableMetaClient metaClient() {
      if (metaClient == null) {
        metaClient = StreamerUtil.createMetaClient(conf);
      }
      return metaClient;
    }

    private String describe() {
      return "table " + conf.get(FlinkOptions.PATH) + ", filter " + filter();
    }
  }

  // -------------------------------------------------------------------------
  //  Partition stage
  // -------------------------------------------------------------------------

  /**
   * Replaces (ON) or shadows (COMPARE) Flink's partition stats pruner ({@code ColumnStatsPartitionPruner}).
   */
  public static final class PartitionStatsPlanner implements PartitionPruners.PartitionPruner {
    private static final long serialVersionUID = 1L;

    private final IndexPlannerMode mode;
    private final PartitionPruners.PartitionPruner engine;
    private final Configuration conf;
    private final RowType rowType;
    private final ColumnStatsProbe probe;
    private final PartitionStatsIndex metadataSource;
    @Nullable
    private HoodieTableMetaClient metaClient;

    /**
     * @param engine Flink's partition stats pruner, run in COMPARE mode only
     */
    public PartitionStatsPlanner(IndexPlannerMode mode, PartitionPruners.PartitionPruner engine, RowType rowType, String basePath,
                                 Configuration conf, ColumnStatsProbe probe, @Nullable HoodieTableMetaClient metaClient) {
      this.mode = mode;
      this.engine = engine;
      this.conf = conf;
      this.rowType = rowType;
      this.probe = probe;
      this.metaClient = metaClient;
      this.metadataSource = new PartitionStatsIndex(basePath, rowType, conf, metaClient);
    }

    @Override
    public Set<String> filter(Collection<String> partitions) {
      Set<String> all = new LinkedHashSet<>(partitions);
      if (mode == IndexPlannerMode.COMPARE) {
        Set<String> engineResult = engine.filter(partitions);
        try {
          CandidateFiles planner = lookup(all);
          IndexPlannerComparison comparison = IndexPlannerComparison.compare(all, Option.of(engineResult), planner);
          comparison.log("partition stats of " + describe());
          report(new Report(Stage.PARTITIONS, mode, probe.getPlannerFilter(), all, engineResult, planner, comparison, null));
        } catch (Throwable t) {
          log.warn("Index planner COMPARE (partition stats) failed for {}, ignored", describe(), t);
          report(new Report(Stage.PARTITIONS, mode, probe.getPlannerFilter(), all, engineResult, null, null, t));
        }
        return engineResult;
      }
      CandidateFiles candidates;
      Throwable failure = null;
      try {
        candidates = lookup(all);
      } catch (Throwable t) {
        log.warn("Index planner (partition stats) failed for {}, no pruning from the planner", describe(), t);
        candidates = CandidateFiles.all();
        failure = t;
      }
      Set<String> result = new HashSet<>(all);
      if (!candidates.isAll()) {
        result.retainAll(candidates.fileNames());
      }
      log.info("Index planner partition stats pruning for {}: {} partitions, {} left", describe(), all.size(), result.size());
      report(new Report(Stage.PARTITIONS, mode, probe.getPlannerFilter(), all, null, candidates, null, failure));
      return result;
    }

    private CandidateFiles lookup(Set<String> partitions) {
      Expression filter = probe.getPlannerFilter();
      if (filter == null || partitions.isEmpty()) {
        return CandidateFiles.all();
      }
      HoodieSchema schema = schemaOf(rowType);
      List<IndexLookup> plan = newPlanner().plan(filter, schema, EnumSet.of(MetadataPartitionType.PARTITION_STATS));
      // the files are not listed yet: each partition stands for its files
      Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
      partitions.forEach(partition -> filesByPartition.put(partition, Collections.singletonList(partition)));
      HoodieTableMetadata metadata = metadataSource.getMetadataTable();
      IndexLookupContext context = new IndexLookupContext(metaClient(), metadata, HoodieFlinkEngineContext.DEFAULT,
          schema, HudiStatsEvaluator.getInstance(), filesByPartition);
      return DefaultIndexPlanner.lookupAll(plan, filter, context);
    }

    private HoodieTableMetaClient metaClient() {
      if (metaClient == null) {
        metaClient = StreamerUtil.createMetaClient(conf);
      }
      return metaClient;
    }

    private String describe() {
      return "table " + conf.get(FlinkOptions.PATH) + ", filter " + probe.getPlannerFilter();
    }

    @Override
    public void close() {
      engine.close();
      metadataSource.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static DefaultIndexPlanner newPlanner() {
    // Flink resolves the encoding of a single-field complex key as its own record index support does
    return new DefaultIndexPlanner(new RecordIndexLookup(KeyGenUtils::resolveComplexKeyGenEncoding));
  }

  private static Set<String> filesOf(List<FileSlice> fileSlices) {
    Set<String> files = new LinkedHashSet<>();
    fileSlices.forEach(fileSlice -> files.addAll(fileSlice.getAllFileNames()));
    return files;
  }

  private static List<FileSlice> slicesWithAnyOf(List<FileSlice> fileSlices, Set<String> files) {
    return fileSlices.stream()
        .filter(fileSlice -> fileSlice.getAllFileNames().stream().anyMatch(files::contains))
        .collect(Collectors.toList());
  }

  /**
   * The files of the slices that are read with the candidates: a slice is read as a whole when any of its files is
   * kept.
   */
  private static CandidateFiles keptFiles(List<FileSlice> fileSlices, CandidateFiles candidates) {
    return candidates.isAll() ? candidates : CandidateFiles.of(filesOf(slicesWithAnyOf(fileSlices, candidates.fileNames())));
  }

  private static void report(Report report) {
    Consumer<Report> listener = testListener;
    if (listener != null) {
      listener.accept(report);
    }
  }

  /**
   * Receives every COMPARE and ON result, for tests; null to stop.
   */
  @VisibleForTesting
  public static void setTestListener(@Nullable Consumer<Report> listener) {
    testListener = listener;
  }

  /**
   * Where the planner ran.
   */
  public enum Stage {
    /**
     * Partition pruning with partition stats; the candidates are partition paths.
     */
    PARTITIONS,
    /**
     * File slice pruning with the record index and column stats; the candidates are file names.
     */
    FILES
  }

  /**
   * One planner run, for tests.
   */
  public static final class Report {
    private final Stage stage;
    private final IndexPlannerMode mode;
    private final Expression filter;
    private final Set<String> candidates;
    private final Set<String> engineKept;
    private final CandidateFiles plannerKept;
    private final IndexPlannerComparison comparison;
    private final Throwable failure;

    Report(Stage stage, IndexPlannerMode mode, Expression filter, Set<String> candidates, Set<String> engineKept,
           CandidateFiles plannerKept, IndexPlannerComparison comparison, Throwable failure) {
      this.stage = stage;
      this.mode = mode;
      this.filter = filter;
      this.candidates = candidates;
      this.engineKept = engineKept;
      this.plannerKept = plannerKept;
      this.comparison = comparison;
      this.failure = failure;
    }

    public Stage stage() {
      return stage;
    }

    public IndexPlannerMode mode() {
      return mode;
    }

    @Nullable
    public Expression filter() {
      return filter;
    }

    /**
     * The candidates before the planner's indexes: partition paths or file names.
     */
    public Set<String> candidates() {
      return candidates;
    }

    /**
     * Flink's own result (COMPARE only).
     */
    @Nullable
    public Set<String> engineKept() {
      return engineKept;
    }

    /**
     * The planner's result; null if it failed in COMPARE mode.
     */
    @Nullable
    public CandidateFiles plannerKept() {
      return plannerKept;
    }

    /**
     * The planner's result as a set; every candidate if it did not prune.
     */
    public Set<String> plannerKeptSet() {
      return plannerKept == null || plannerKept.isAll() ? candidates : plannerKept.fileNames();
    }

    @Nullable
    public IndexPlannerComparison comparison() {
      return comparison;
    }

    @Nullable
    public Throwable failure() {
      return failure;
    }

    @Override
    public String toString() {
      return stage + " " + mode + " filter=" + filter + " candidates=" + candidates.size()
          + (engineKept == null ? "" : " engineKept=" + engineKept.size())
          + " plannerKept=" + (plannerKept == null ? "failed" : plannerKeptSet().size())
          + (comparison == null ? "" : " onlyPlanner=" + comparison.prunedOnlyByPlanner() + " onlyEngine=" + comparison.prunedOnlyByEngine())
          + (failure == null ? "" : " failure=" + failure);
    }
  }
}
