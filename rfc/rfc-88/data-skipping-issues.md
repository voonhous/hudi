<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-88: outstanding data skipping bugs

Bugs in today's data skipping that the RFC-88 index planner work (step 5) found and that are **not fixed on master**.
The planner itself handles each case correctly; these entries are about the engines' existing code, which stays the
default (`hoodie.metadata.index.planner.mode=OFF`) until COMPARE shows no differences. Update this file when an item is
fixed upstream or filed as an issue.

These came up while wiring the engine-neutral index planner (RFC-88 step 5, branch [`voonhous:claude/rfc-88-pr-confirmation-wztvyi`](https://github.com/voonhous/hudi/tree/claude/rfc-88-pr-confirmation-wztvyi)) into Spark, Flink and Hive. Its oracle tests compare pruned reads against unpruned ones. Each item below was checked in the code at master `b0d16f9`, and none is fixed on master. Line links point at that commit.

**Wrong results (files or partitions with matching rows are pruned)**

| # | Engine | Bug | Where |
|---|---|---|---|
| 1 | Flink | `NOT` negates the stats answer instead of the predicate. For a file with `min=1, max=10`, `a < 5` "might match", so `NOT (a < 5)` prunes the file even though it holds rows with `a >= 5`. | [`ExpressionEvaluators.Not#eval`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/ExpressionEvaluators.java#L473) |
| 2 | Flink | `AND` and `OR` only evaluate their first two operands. An `OR` with three or more operands prunes files that match only a later operand. | [`ExpressionEvaluators#fromExpression`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/ExpressionEvaluators.java#L82-L94) |
| 3 | Flink | Any null literal turns the whole leaf into `AlwaysFalse`, so `a IN (1, NULL)` prunes every file, although rows with `a = 1` match. `In#eval` also returns false as soon as one value is null. | [`ExpressionEvaluators#fromExpression`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/ExpressionEvaluators.java#L107-L113) |
| 4 | hudi-common (partition listing with a filter expression) | `PartialBindVisitor` binds a predicate on a column missing from the partial partition schema to `TRUE` ("unknown"), then negates it to `FALSE`. On `region=us\|eu/hh=1\|2\|3`, `NOT (hh = 1)` prunes every partition at the first level, losing `region=us/hh=2` and `region=eu/hh=3`. This is fixed on the RFC-88 branch in [bcd3c5f](https://github.com/voonhous/hudi/commit/bcd3c5f) with a regression test, but not on master. | [`PartialBindVisitor#visitPredicate`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-common/src/main/java/org/apache/hudi/common/expression/PartialBindVisitor.java#L80-L81) |

**Missed pruning (results are correct, but more files are read)**

| # | Engine | Bug | Where |
|---|---|---|---|
| 5 | Flink | DATE stats are read with `getLong`, but they are stored as int. The `ClassCastException` is caught in `FileStatsIndex#computeCandidateFiles`, so DATE filters never prune. | [`ColumnStatsProbe`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/prune/ColumnStatsProbe.java#L122) |
| 6 | Flink / Java writers | Known as HUDI-8801: `updateColumnsToIndexWithColStats` is a no-op, so the source fields of the column stats index definition stay empty when column stats are enabled at table creation. Spark's `ColumnStatsIndexSupport` only uses the listed columns, so on such tables it uses no column or partition stats. | [`HoodieFlinkWriteClient`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/client/HoodieFlinkWriteClient.java#L556) |
| 7 | Flink | Flink skips the partitioned record index above 3 candidate partitions, while Spark allows 10. | [`RecordLevelIndex.MAX_PARTITIONS`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/stats/RecordLevelIndex.java#L69) |

**Failures (indexing throws)**

| # | Engine | Bug | Where |
|---|---|---|---|
| 8 | hudi-common (column stats from Parquet footers) | `LogicalTypeParquetAdapter#getValueTypeFromAnnotation` handles String, Decimal, Date, Time, Timestamp and UUID annotations and throws `Unsupported logical type annotation` for anything else, including `INT(8)` / `INT(16)`. Spark's row writer annotates `ShortType` columns `INT32 + INT(16)` today, so column stats over such files throw. Found by RFC-88 step 2b (MOR writes failed once files carried `INT(8)`), and fixed on the RFC-88 branch in b5eae82 (with a `TestValueType` case), not on master. | [`LogicalTypeParquetAdapter`](https://github.com/apache/hudi/blob/b0d16f94b3b5f4ce790a9a09e41ebf891b35a53e/hudi-common/src/main/java/org/apache/hudi/metadata/stats/LogicalTypeParquetAdapter.java#L68) |

**Wrong stored values**

| # | Engine | Bug | Where |
|---|---|---|---|
| 9 | Flink | A table whose schema stores a `TINYINT` or `SMALLINT` column as `INT` (no explicit schema, or one that says `int`) stores plain `INT32` holding the raw byte pattern: -128 is stored as 128, and -1 as 255 or 65535. Other engines read the stored numbers, and the column stats are those numbers. `WHERE f_tiny < 0` returns no rows in Flink even with data skipping off, because the Parquet row-group filter prunes on those stats. **New writes are fixed on the RFC-88 branch** (`fix(flink): store TINYINT and SMALLINT values sign extended`); existing files are not, see below. | The sink's rows keep the `TINYINT` / `SMALLINT` layout of the table definition, while the writers (Parquet, Lance, Avro log blocks) read each column with the width of the write schema: a binary row's one- or two-byte value read as an int is zero extended. Fixed in `HoodieTableSink`, which widens these columns to the write schema's integer type first (`NarrowIntWidening`) |

Item 9, what the fix covers and what remains:

- **New writes** (every table, old and new, every write path through the Flink SQL / DataStream sink: upsert, insert,
  bulk insert, append; COW and MOR; Parquet, Lance and log blocks) store the sign-extended value, so other engines read
  -1 and the column stats hold -1.
- **Existing files keep the unsigned values**, and rewriting a file (a COW merge, compaction, clustering) copies them as
  stored, because those read and write the column as `INT`. Only rewriting the rows through Flink SQL with the
  `TINYINT` / `SMALLINT` definition (for example `INSERT OVERWRITE ... SELECT`) repairs them. Spark, Hive and other
  engines keep reading 128 / 255 / 65535 from them; their own stats agree with what they read, so their pruning is not
  wrong, only the values are.
- **Flink reads of existing files stay correct where Flink can read the column**: it reads the `INT32` value truncated
  to `TINYINT` / `SMALLINT`, and truncation gives the same byte for 255 and -1. That is the COW Parquet path. MOR
  (file group reader, Parquet and log files) and Lance reads of a `TINYINT` / `SMALLINT` column whose table schema
  says `INT` fail with a `ClassCastException` (`Integer` cannot be cast to `Byte`) on master and still do; this does
  not depend on the stored values. Tables whose schema has `INT8` / `INT16` (explicit schema, or new tables from
  RFC-88 step 2c) read on every path.
- **Stats of existing files**: an old file's stored values are `v & 0xFF` (`v & 0xFFFF`), so its stats lie within
  -128..127 (-32768..32767) exactly when it holds no negative value, and then they equal the values Flink reads; a file
  with a negative value has a max of 128 (32768) or more. Flink therefore treats `TINYINT` / `SMALLINT` stats out of
  the type's range as unknown and keeps the file: in the Parquet row-group filter (`ExpressionPredicates`, a
  user-defined predicate that compares the truncated values) and in the column stats and partition stats indexes
  (`FileStatsIndex`). This also covers wider values another engine wrote to the `INT` column, which Flink reads
  truncated as well. The RFC-88 index planner (non-default modes) does not use filters on `TINYINT` / `SMALLINT`
  columns stored as `INT` (`IndexPlannerSupport`). Before the fix, the column stats index never pruned on
  `TINYINT` / `SMALLINT` columns (`ColumnStatsProbe` read their stats with `getInt`, the `ClassCastException` was caught
  and nothing was pruned); it now prunes with in-range stats.
- Since RFC-88 step 2c, a table created from now on stores `TINYINT` / `SMALLINT` as `INT8` / `INT16` (`INT32` +
  `INT(8)` / `INT(16)`), so none of this applies to it; tables created before keep `INT` and rely on the fix above.
- Tested in `ITTestNarrowIntStoredValues`: stored values, Parquet stats, reads and filters with data skipping on and
  off, for new writes and for a table holding pre-fix files, including a file that merges old and new rows.

**Already tracked in apache/hudi#20449, with one more reproduction**

- [apache/hudi#20430](https://github.com/apache/hudi/issues/20430) (`LIKE 'prefix%'`): reproduced on a MOR table. Log files that hold only `beta-00xx` values are pruned for `s LIKE 'beta%'`, and the query returned 0 of 50 matching rows. On the COW variant of the same data, the updated base files still held `alpha-` values, so their minimum fell below the prefix and the bug did not show.

Spark issues are tracked in the umbrella [apache/hudi#20449](https://github.com/apache/hudi/issues/20449); items 1–3 and 5–7 are Flink-side and have no issue yet; items 8 and 9 have none either.

