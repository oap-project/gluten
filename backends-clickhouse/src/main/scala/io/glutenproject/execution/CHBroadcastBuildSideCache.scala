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

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.vectorized.StorageJoinBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation}

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}

import java.util.concurrent.TimeUnit

case class BroadcastHashTable(pointer: Long, relation: ClickHouseBuildSideRelation)

/**
 * `CHBroadcastBuildSideCache` is used for controlling to build bhj hash table once.
 *
 * The complicated part is due to reuse exchange, where multiple BHJ IDs correspond to a
 * `ClickHouseBuildSideRelation`.
 */
object CHBroadcastBuildSideCache extends Logging {

  private def threadLog(msg: => String): Unit =
    logDebug(s"Thread: ${Thread.currentThread().getId} -- $msg")

  private lazy val expiredTime = SparkEnv.get.conf.getLong(
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME,
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT
  )

  // Use for controlling to build bhj hash table once.
  // key: hashtable id, value is hashtable backend pointer(long to string).
  private val buildSideRelationCache: Cache[String, BroadcastHashTable] =
    CacheBuilder.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(
        (notification: RemovalNotification[String, BroadcastHashTable]) => {
          cleanBuildHashTable(notification.getKey, notification.getValue)
        })
      .build[String, BroadcastHashTable]()

  def getOrBuildBroadcastHashTable(
      broadcast: Broadcast[BuildSideRelation],
      broadCastContext: BroadCastHashJoinContext): BroadcastHashTable = {

    buildSideRelationCache
      .get(
        broadCastContext.buildHashTableId,
        () => {
          val (pointer, relation) =
            broadcast.value
              .asInstanceOf[ClickHouseBuildSideRelation]
              .buildHashTable(broadCastContext)
          threadLog(s"create bhj ${broadCastContext.buildHashTableId} = 0x${pointer.toHexString}")
          BroadcastHashTable(pointer, relation)
        }
      )
  }

  /** This is callback from c++ backend. */
  def get(broadcastHashtableId: String): Long =
    Option(buildSideRelationCache.getIfPresent(broadcastHashtableId))
      .map(_.pointer)
      .getOrElse(0)

  def invalidateBroadcastHashtable(broadcastHashtableId: String): Unit = {
    // Cleanup operations on the backend are idempotent.
    buildSideRelationCache.invalidate(broadcastHashtableId)
  }

  /** Only used in UT. */
  def size(): Long = buildSideRelationCache.size()

  private def cleanBuildHashTable(key: String, value: BroadcastHashTable): Unit = {
    threadLog(s"remove bhj $key = 0x${value.pointer.toHexString}")
    if (value.relation != null) {
      value.relation.reset()
    }
    StorageJoinBuilder.nativeCleanBuildHashTable(key, value.pointer)
  }
}
