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
package org.apache.gluten.vectorized

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An Iterator that insures that the batches [[ColumnarBatch]]s it iterates over are all closed
 * properly.
 */
class CloseablePartitionedBlockIterator(itr: Iterator[Product2[Int, ColumnarBatch]])
  extends Iterator[Product2[Int, ColumnarBatch]]
  with Logging {
  var cb: ColumnarBatch = null

  override def hasNext: Boolean = {
    itr.hasNext
  }

  TaskContext.get().addTaskCompletionListener[Unit] {
    _ =>
      {
        closeCurrentBatch()
        if (itr.isInstanceOf[AutoCloseable]) itr.asInstanceOf[AutoCloseable].close()
      }
  }

  override def next(): Product2[Int, ColumnarBatch] = {
    closeCurrentBatch()
    val value = itr.next()
    cb = value._2
    value
  }

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      CHNativeBlock.closeFromColumnarBatch(cb)
      cb = null
    }
  }
}
