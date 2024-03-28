# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
batchsize=20480

export GLUTEN_HOME=/PATH/TO/gluten/
#please choose right os system jar
export GLUTEN_JVM_JAR=${GLUTEN_HOME}/package/target/<gluten-jar>
SPARK_HOME=/home/sparkuser/spark/

cat tpch_convert_parquet_dwrf.scala  | ${SPARK_HOME}/bin/spark-shell                            \
 --name convert_parquet_dwrf                                      \
 --master yarn                                             \
 --deploy-mode client                                      \
 --driver-memory 20g                                       \
 --executor-cores 8                                      \
 --num-executors 14                                       \
 --executor-memory 30g                                   \
 --conf spark.plugins=org.apache.gluten.GlutenPlugin        \
 --conf spark.driver.extraClassPath=${GLUTEN_JVM_JAR}      \
 --conf spark.executor.extraClassPath=${GLUTEN_JVM_JAR}    \
 --conf spark.memory.offHeap.size=30g                      \
 --conf spark.executor.memoryOverhead=5g                   \
 --conf spark.driver.maxResultSize=32g                     \
 --conf spark.sql.autoBroadcastJoinThreshold=-1            \
 --conf spark.sql.parquet.columnarReaderBatchSize=${batchsize} \
 --conf spark.sql.inMemoryColumnarStorage.batchSize=${batchsize} \
 --conf spark.sql.execution.arrow.maxRecordsPerBatch=${batchsize} \
 --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
 --conf spark.sql.broadcastTimeout=4800 \
 --conf spark.driver.maxResultSize=4g \
 --conf spark.sql.adaptive.enabled=true \
 --conf spark.sql.shuffle.partitions=112 \
 --conf spark.sql.sources.useV1SourceList=avro             \
 --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
 --conf spark.sql.files.maxPartitionBytes=1073741824 \
