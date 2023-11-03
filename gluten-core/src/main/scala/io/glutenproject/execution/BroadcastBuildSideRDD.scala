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
package io.glutenproject.execution

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class BroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    broadCastContext: BroadCastHashJoinContext)
  extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    throw new IllegalStateException("Never reach here")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    throw new IllegalStateException("Never reach here")
  }
}
