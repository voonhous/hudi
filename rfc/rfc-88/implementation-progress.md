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
# RFC-88 implementation progress

What has been implemented of [RFC-88](rfc-88.md) on branch `claude/rfc-88-pr-confirmation-wztvyi`, the bugs and
problems found along the way, and what is still open. The branch is based on `apache/hudi` master `b0d16f9`; the RFC
text is the head of cshuo/hudi#24 (`18e09e9`). Outstanding data skipping bugs in today's code have their own file,
[data-skipping-issues.md](data-skipping-issues.md).

## Progress

| Rollout step | Status |
|---|---|
| 1. Mappers | Done |
| 2. Converge mappings | In progress: 2a, 2b and 2c done for INT8 / INT16 / CHAR; caller switch-over and the remaining RFC-99 types open |
| 3. `HoodieRow` with its first format | Not started |
| 4. One format at a time | Not started |
| 5. Expressions | Mostly done: translators, renderers, index planner, engine wiring behind a flag |

### Step 1: mappers (no behaviour change)

| Part | Commits | Notes |
|---|---|---|
| `TypeSupport`, `TypeSystemMapper`, `FormatSchemaMapper`, options, `SchemaMappers` (ServiceLoader) registry, `AvroSchemaMapper` | 3737c5f | Format mappers are keyed by name, not `HoodieFileFormat` (adding `AVRO` there would change base-file extensions) |
| Shared contract test kit (samples per type and shape, structural equivalence, strict known gaps) | 3737c5f, dec73eb | Known gaps are the bug list; a fixed gap fails the build until removed |
| `HoodieSchemaHints` (restore LOSSY types from the table schema) | b3426ae | |
| `CatalogSchemaRenderer` interface and registry | e45e1ad | `diff` takes partition fields and options |
| Spark, Flink, Hive, BigQuery type mappers; Parquet, ORC format mappers; Hive, Spark-properties, BigQuery renderers | 38686d4, ac426d5, 3dae206, c366e2d, 168a73c, ddf20cd | Each wraps today's converter |

### Step 2: converge mappings

| Part | Commits | Notes |
|---|---|---|
| 2a. `HoodieSchemaType.INT8`, `INT16`, `CHAR(n)`; Avro forms `{"type":"int","logicalType":"int16"}`, `{"type":"string","logicalType":"char","length":n,"maxLength":n}`; every site that handles INT / STRING accepts them | 6cc0e8d, cd15dd0 | Older readers ignore the logical type and read int / string; `maxLength` lets Hive's AvroSerDe read char(n) |
| 2b. HoodieSchema -> target maps the types natively: Parquet INT(8) / INT(16), ORC and Hive tinyint / smallint / char(n), catalog sync, Spark ByteType / ShortType / CHAR, Flink TINYINT / SMALLINT / CHAR(n) | 1f01445, c52884b, f6fc99a, b5eae82, af4bc4e | End-to-end tests read tables with these types through Spark, Flink and Hive |
| `hoodie.table.type.system.version`: absent (0) = today's mapping, 1 = INT8 / INT16 / CHAR; set when a table is created at table version 10 or later | c3fee7e | Implements "existing tables keep existing behaviour, new tables use the new behaviour", including for columns added later |
| 2c. Spark: engine -> HoodieSchema follows the table property (DataFrame writes, bootstrap, CREATE / CTAS, ALTER, INSERT / MERGE, schema on read) | 1dca07a | Compatibility tests: existing tables stay INT / STRING and sync as before; new tables get the native types |
| 2c. Flink: the same (DDL inference, catalogs incl. HMS, readers, schema evolution) | 10b5d92 | 16 compatibility cases: no catalog, DFS catalog and HMS catalog, COW / MOR, new / existing |
| Fix: Flink TINYINT / SMALLINT stored without sign extension | 2773e79 | New writes are correct; reads and pruning guard against old files' values (see data-skipping-issues.md item 9) |
| Switch the remaining callers to the mappers, delete duplicate converters, repair into format mappers, Trino from `HoodieSchema` | | Open |
| Remaining RFC-99 types (unsigned integers, FLOAT16, nanosecond TIMESTAMP, LARGE_STRING / LARGE_BINARY, DATE64, DURATION, DICTIONARY) | | Open |

### Step 5: expressions

| Part | Commits | Notes |
|---|---|---|
| `Expression` typed with `HoodieSchema` instead of InternalSchema types, incl. `HoodieTableMetadata` partition pruning | e1c11d9 | Pure refactor |
| `ExpressionTranslator` / `ExpressionRenderer`, `ConservativeTranslation` (rule 1), contract kit | 917d1d8 | |
| Translators: Spark (`Filter`), Flink (`ResolvedExpression`), Hive (`ExprNodeDesc`) | e621842, 0ecebce, 8eb0a7f | Literals typed from the column; casts stripped only if order preserving |
| Renderers: Spark `Filter`, HMS, Glue, Parquet `FilterPredicate` | e621842, 020ecd6 | |
| Index planner API, mode `hoodie.metadata.index.planner.mode` = OFF (default) / COMPARE / ON | 2eb4648 | COMPARE uses the engine's result and logs differences |
| Planner core: `HudiStatsEvaluator`, column stats / partition stats / record index lookups, `DefaultIndexPlanner` | 0734e1d | Randomized oracle: 2.4M checks, no file with a matching row pruned |
| Wiring into Spark, Flink and Hive (Hive gains data skipping) | e9b97d4, 4363a26, 4b239c2 | Oracle tests: rows with ON equal rows without data skipping in every engine; OFF unchanged |
| Fix: `PartialBindVisitor` pruned matching partitions under NOT | bcd3c5f | |
| Function / aggregate operators for expression indexes; bloom / secondary / expression index lookups; Trino translator | | Open |

## Bugs and problems

### Bugs found in today's code

Data skipping bugs with code links are in [data-skipping-issues.md](data-skipping-issues.md). In short:

| Area | Bug | Effect | Status on this branch |
|---|---|---|---|
| Spark data skipping | `LIKE 'prefix%'` translated as `min <= prefix <= max` (apache/hudi#20430) | Wrong results; on MOR a query returned 0 of 50 matching rows | Not fixed in the old path; the planner is correct |
| Flink data skipping | NOT negates the stats answer; AND / OR read two operands only; any null literal makes the leaf always false | Wrong results | Not fixed in the old path; the planner is correct |
| Flink data skipping | DATE stats read as long; partitioned record index skipped above 3 partitions | Missed pruning | Not fixed in the old path |
| Flink / Java writers | HUDI-8801: column stats index definition never lists its columns | Spark and the planner use no column / partition stats on those tables | Open |
| Flink writes | TINYINT / SMALLINT stored without sign extension (-1 as 255) | Wrong stored values; filters on negative values return nothing | Fixed for new writes (2773e79); old files keep their values |
| Column stats | Parquet INT(8) / INT(16) annotations throw "Unsupported logical type annotation" (Spark ShortType files today) | Indexing fails | Fixed (b5eae82) |
| hudi-common partition pruning | `PartialBindVisitor` turns NOT over an unbindable column into FALSE | Matching partitions pruned | Fixed (bcd3c5f) |
| Spark MERGE INTO | On an existing table, assigning a TINYINT / SMALLINT source column to an INT column fails in `ExpressionPayload` (`MutableInt` to `MutableByte`) | Query fails | Open, also on master |
| Spark MOR | A partial-update MERGE, then ALTER ADD COLUMNS and an insert, reads the updated row's key as null | Wrong results | Open, also on master |
| Flink reads | MOR and Lance reads of a TINYINT / SMALLINT column whose table schema says INT fail with ClassCastException | Query fails | Open, also on master |
| Spark InternalSchema paths | `fixNullOrdering`, schema on read and reconcile convert through InternalSchema, which has no INT8 / INT16 / CHAR | New types lost on every write | Worked around for new tables (types restored after the round trip); InternalSchema itself unchanged (RFC-104) |

### Problems in the work

| Problem | What happened | Handling |
|---|---|---|
| GitHub access | Pushes to `voonhous/hudi` returned 403 until GitHub was reconnected; commenting on apache/hudi#20449 still returns 403 | Comment text kept in the session scratchpad; the list is in data-skipping-issues.md instead |
| JDK 21 only | Scala 2.12.15 crashes on JDK 21 class files | Build with `-Dscala12.version=2.12.18`; Spark 3 modules also need `-Dspark3.5`, which breaks the Hadoop classpath of non-Spark modules |
| Maven repositories | The remote-resources plugin hung on an unreachable repository; Confluent, Cloudera and JitPack do not resolve | `-Dremoteresources.skip=true`; `hudi-utilities` cannot build here, so its changes (one test fixture) are untested |
| No Docker | `hudi-aws` install needs Docker for its integration step | Ran `test` instead of `install` |
| No Spark 4 artifacts offline | Spark 4 module edits are text mirrors of the Spark 3 ones | Not compiled; needs a Spark 4 build |
| Shared 4-core / 15 GB machine | Parallel agents competed for CPU; two Spark test JVMs were killed (exit 137) | Smaller test heaps (`-Xmx1536m`), fewer parallel agents |
| Environment-only test failures | Spark functional tests needing a SparkContext in `hudi-spark-client` fail to start Hadoop; one `TestHoodieSparkSqlWriter` case fails on a non-ASCII file name | Excluded and noted; unrelated to the changes |
| Agent worktrees | Early worktrees started from an old master | Agents reset to the branch before working |
| Contract kit gaps | Nested LOSSY columns checked without the hint; NULL sample passed for the wrong reason; failure messages could throw; a mapper missing its own dependencies broke the registry; markers not required for targets with metadata; nullability cannot be restored for targets without it | All fixed except nullability (dec73eb) |
| Two agents fixing the same file | Spark and Flink both fixed the column stats adapters | Kept one version on cherry-pick and removed duplicate cases |

## Open items

| # | Item | Needs |
|---|---|---|
| 1 | Run COMPARE on real workloads; switch engines to ON and delete the old data skipping code | Real queries; Hive also needs a multi-table query checked end to end |
| 2 | Fix the open wrong-result bugs in today's data skipping (Spark LIKE prefix, Flink NOT, AND / OR arity, null literals), or wait until the old code is deleted | Decision |
| 3 | HUDI-8801: Flink / Java writers should record the column stats index's columns | Fix in the writers |
| 4 | Index planner: unknown mode values should fall back to OFF with a warning; COMPARE should log quietly for engines without their own pruning (Hive) | Small change |
| 5 | Bloom filter, secondary index and expression index lookups in the planner; function / aggregate operators in `Expression`; Trino translator | Step 5 |
| 6 | MOR + schema evolution on a new Flink table cannot read INT8 / INT16 / CHAR columns: `FileGroupReaderSchemaHandler.getRequiredSchemaForInstantAndRenamedColumns` converts back through InternalSchema | Fix in hudi-common, applies to every engine |
| 7 | HoodieStreamer (`hudi-utilities`) still uses today's mapping for new tables; bootstrap from ORC sources not switched | Switch where the module builds |
| 8 | Standalone Flink compaction / clustering jobs read file schemas with the old mapping | Pass the table property |
| 9 | Restoring nullability from the hint for targets without nullability (Hive TypeInfo, ORC); about 120 known gaps | Step 2 |
| 10 | Parquet and ORC markers need file metadata in the mapper's physical type (`hoodie.vector.columns`, `orc.avro.schema`) | Design for step 2 |
| 11 | Parquet writes do not range-check INT8 / INT16 values (ORC does) | Small change |
| 12 | Existing tables cannot migrate a column to INT16 / CHAR; an explicit `ALTER TABLE ... ALTER COLUMN ... TYPE SMALLINT` could do it as a metadata change | Decision |
| 13 | Existing Flink files written with the sign-extension bug keep wrong values for other engines | Rewrite through Flink; document for users |
| 14 | Spark 4 builds untested; `hudi-utilities` fixture change untested | Builds in CI |
| 15 | Steps 3 (`HoodieRow`, ORC through `FileFormatIO`) and 4 (one format at a time, with benchmarks) | Not started |
| 16 | The Spark MERGE INTO ClassCastException and the MOR partial-update null key, both also on master | Separate fixes |
