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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}

class Spark34Scan extends DataSourceV2ScanExecBase {

  override def scan: Scan = throw new UnsupportedOperationException("Spark34Scan")

  override def ordering: Option[Seq[SortOrder]] = throw new UnsupportedOperationException(
    "Spark34Scan")

  override def readerFactory: PartitionReaderFactory =
    throw new UnsupportedOperationException("Spark34Scan")

  override def keyGroupedPartitioning: Option[Seq[Expression]] =
    throw new UnsupportedOperationException("Spark34Scan")

  override protected def inputPartitions: Seq[InputPartition] =
    throw new UnsupportedOperationException("Spark34Scan")

  override def inputRDD: RDD[InternalRow] = throw new UnsupportedOperationException("Spark34Scan")

  override def output: Seq[Attribute] = throw new UnsupportedOperationException("Spark34Scan")

  override def productElement(n: Int): Any = throw new UnsupportedOperationException("Spark34Scan")

  override def productArity: Int = throw new UnsupportedOperationException("Spark34Scan")

  override def canEqual(that: Any): Boolean = throw new UnsupportedOperationException("Spark34Scan")

}
