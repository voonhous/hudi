<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# RFC-88 HoodieRow write benchmarks (prototype, not for merge)

JMH benchmarks for RFC-88's data plane: engine rows written to Parquet through a `HoodieRow` view and one shared
Parquet writer, against master's Spark and Flink Parquet writers.

## What is here

`org.apache.hudi.benchmarks.rowwriter`:

- `HoodieGetters`, `HoodieRow`, `HoodieArray`, `BinaryRef`: the boundary. Getters return primitives or a borrowed
  byte slice; no engine object leaves a wrapper.
  - DECIMAL p <= 18: `getDecimalUnscaledLong`; p > 18: `getDecimalUnscaledBytes` (unscaled, big-endian).
  - STRING / BINARY: `getBinaryRef(i, ref)` sets `{array, offset, length}` over the engine's own bytes; valid until
    the next call.
  - TIMESTAMP: `getLong` (micros); DATE: `getInt` (days); structs and arrays: a reused child wrapper.
- `SparkRowWrapper` / `SparkArrayWrapper` (over `InternalRow` / `ArrayData`), `AvroRowWrapper` / `AvroArrayWrapper`
  (over `GenericRecord`), and in `hudi-flink-benchmarks`, `FlinkRowWrapper` / `FlinkArrayWrapper` (over `RowData`).
  One wrapper per row type, reused across rows.
- `HoodieRowParquetWriter`: one Parquet writer for every engine. Value writers come from the Parquet schema alone.
- `HoodieRowParquetWriterMatched`: control only, not proposed. The same boundary with exactly today's Spark
  conversions (string `getBytes()` copy, every decimal through `BigDecimal`, Spark's rebase functions, a capturing
  `Runnable` per message, group and field), so the only difference from today is the boundary.
- `ParquetWriteBenchmark` (Spark): today (`HoodieRowParquetWriteSupport`) vs `HoodieRow` vs control.
- `ParquetWriteMegamorphicBenchmark` (Spark): the `HoodieRow` writer after its call sites have seen four wrapper
  classes (Spark and Avro, rows and arrays).
- `FlinkParquetWriteBenchmark`: today (`RowDataParquetWriteSupport` / `ParquetRowDataWriter`) vs `HoodieRow`.
- `SmallIntCheck` (Flink): writes SMALLINT -1, 300, -300, 32767, -32768 through both writers and reads them back.

Every benchmark's setup asserts that all writers produce byte-identical files for every workload, and that Spark rows
and the same data as Avro records produce byte-identical files through the one writer.

Workloads (20,000 rows per file): DECIMAL (8 x DECIMAL(10,2), 2 x DECIMAL(30,4)); STRING (8 strings, 16-256 bytes);
NESTED (struct, array of structs, array of strings); MIXED (short, int, long, double, string, decimal, timestamp,
date, array; Flink's MIXED has no short, since today's Flink writer cannot write one, see `SmallIntCheck`).

## Build and run (JDK 17)

```sh
export JAVA_HOME=<JDK 17>
# Spark
mvn -T 4 -Pspark3.5 -pl hudi-benchmarks -am -DskipTests -Dcheckstyle.skip -Drat.skip=true -Dscalastyle.skip=true \
  -Dspotless.check.skip=true -Denforcer.skip=true -Dmaven.javadoc.skip=true -Dmaven.source.skip=true \
  package dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=runtime
hudi-benchmarks/run-jmh.sh -prof gc                               # BENCH=Megamorphic runs only the megamorphic case
# Flink
mvn -T 4 -Pspark3.5,flink2.2 -pl hudi-flink-benchmarks -am -DskipTests -Dcheckstyle.skip -Drat.skip=true \
  -Dscalastyle.skip=true -Dspotless.check.skip=true -Denforcer.skip=true -Dmaven.javadoc.skip=true \
  -Dmaven.source.skip=true \
  package dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=runtime
hudi-flink-benchmarks/run-jmh.sh -prof gc
MAIN=org.apache.hudi.benchmarks.rowwriter.SmallIntCheck hudi-flink-benchmarks/run-jmh.sh
```

`-am` is required: building the module alone picks up stale Hudi jars from `~/.m2`. The Spark 3.5 and Flink 2.2
profiles must be named explicitly on JDK 17. `hudi-flink-benchmarks` pins every Parquet jar to Flink's 1.15.2 and
logs at WARN; Parquet's debug logging otherwise slows the Flink numbers 10-20x.

## Results

JDK 17.0.14 (Temurin), Apple Silicon laptop, uncompressed, in-memory output, 2 forks x 5 iterations x 3 s after
3 x 3 s warmup, `-prof gc`. Time per row in ns, JMH 99.9% error.

Spark 3.5, Parquet 1.13.1:

| Workload | Today (master) | HoodieRow | Control: boundary only | HoodieRow, four row types in one JVM |
|---|---|---|---|---|
| DECIMAL | 754 +/- 249 | 473 +/- 6 | 787 +/- 236 | 476 +/- 2 |
| STRING | 2060 +/- 60 | 1935 +/- 61 | 2150 +/- 151 | 1870 +/- 25 |
| NESTED | 2146 +/- 76 | 2064 +/- 41 | 2273 +/- 45 | 2065 +/- 30 |
| MIXED | 1171 +/- 6 | 1133 +/- 10 | 1195 +/- 6 | 1113 +/- 26 |

Allocation per row (bytes), today vs HoodieRow: DECIMAL 2508 vs 1372, STRING 5819 vs 4659, NESTED 7484 vs 5781,
MIXED 2807 vs 2417. The control allocates about what today does.

Flink 2.2.1, Parquet 1.15.2:

| Workload | Today (master) | HoodieRow |
|---|---|---|
| DECIMAL | 624 +/- 100 | 587 +/- 77 |
| STRING | 2803 +/- 567 | 2435 +/- 311 |
| NESTED | 2648 +/- 207 | 2188 +/- 86 |
| MIXED (no SMALLINT) | 1357 +/- 103 | 1218 +/- 47 |

Allocation per row (bytes), today vs HoodieRow: DECIMAL 1771 vs 1331, STRING 6005 vs 4940, NESTED 6942 vs 5637,
MIXED 2636 vs 2292.

Reading them:

- The boundary alone (control) costs about 2-6% (MIXED +2%, NESTED +6%; DECIMAL and STRING within error).
- The shared writer more than recovers it: unscaled-long decimals, string slices instead of copies, no closure per
  field. Today's writers could adopt each of these in place; with HoodieRow each is written once for every engine.
- Megamorphic call sites cost nothing measurable.
- `SmallIntCheck`: today's Flink writer reads -1, 300, -300, 32767, -32768 back as 65535, 300, 65236, 32767, 32768
  from `BinaryRowData`, and fails with `ClassCastException` on `GenericRowData`; the HoodieRow writer round-trips them.

Not covered: reads, compression and real IO, other formats, the Flink control and megamorphic cases, other machines.
