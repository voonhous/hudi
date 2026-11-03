#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Runs the Flink Parquet write benchmark, or a main class with MAIN=... (e.g. SmallIntCheck).
# Build first (see ../hudi-benchmarks/README.md). Usage: run-jmh.sh [JMH args...]
set -e
cd "$(dirname "$0")"
JAVA="${JAVA_HOME:?set JAVA_HOME to a JDK 17}/bin/java"
OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
CP="target/classes:$(cat target/classpath.txt)"
if [ -n "$MAIN" ]; then
  exec "$JAVA" $OPENS -cp "$CP" "$MAIN" "$@"
fi
exec "$JAVA" $OPENS -cp "$CP" org.openjdk.jmh.Main "${BENCH:-FlinkParquetWrite}" -jvmArgsAppend "-Xms4g -Xmx4g $OPENS" "$@"
