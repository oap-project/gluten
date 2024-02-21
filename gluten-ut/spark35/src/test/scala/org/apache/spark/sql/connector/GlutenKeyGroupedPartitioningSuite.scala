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
package org.apache.spark.sql.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestConstants, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructType, TimestampType}

import java.util.Collections

class GlutenKeyGroupedPartitioningSuite
  extends KeyGroupedPartitioningSuite
  with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "5")
  }

  private val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }
  private def createTable(
      table: String,
      schema: StructType,
      partitions: Array[Transform],
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(
      Identifier.of(Array("ns"), table),
      schema,
      partitions,
      emptyProps,
      Distributions.unspecified(),
      Array.empty,
      None,
      numRowsPerSplit = 1)
  }

  private def collectShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    // here we skip collecting shuffle operators that are not associated with SHJ
    collect(plan) { case s: ShuffledHashJoinExec => s }.flatMap(
      smj => collect(smj) { case s: ShuffleExchangeExec => s })
  }

  private def collectScans(plan: SparkPlan): Seq[BatchScanExec] = {
    collect(plan) { case s: BatchScanExec => s }
  }

  private val items: String = "items"
  private val items_schema: StructType = new StructType()
    .add("id", LongType)
    .add("name", StringType)
    .add("price", FloatType)
    .add("arrive_time", TimestampType)
  private val purchases: String = "purchases"
  private val purchases_schema: StructType = new StructType()
    .add("item_id", LongType)
    .add("price", FloatType)
    .add("time", TimestampType)

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-41413: partitioned join: partition values" +
      " from one side are subset of those from the other side") {
    val items_partitions = Array(bucket(4, "id"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(4, "item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)

    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-41413: partitioned join:" +
      " partition values from both sides overlaps") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp)), " +
        "(4, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 19.5)))
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-41413: partitioned join:" +
      " non-overlapping partition values from both sides") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(4, 42.0, cast('2020-01-01' as timestamp)), " +
        "(5, 19.5, cast('2020-02-01' as timestamp)), " +
        "(6, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq.empty)
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered:" +
      " with same partition keys and one side fully clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.forall(_.inputRDD.partitions.length == expected))
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered:" +
      " with same partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 22.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.forall(_.inputRDD.partitions.length == expected))
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 22.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered: with different" +
      " partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 10), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.forall(_.inputRDD.partitions.length == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 25.0),
                  Row(2, "bb", 10.0, 30.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered: with different" +
      " partition keys and missing keys on left-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 9), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.forall(_.inputRDD.partitions.length == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered:" +
      " with different partition keys and missing keys on right-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 6), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.forall(_.inputRDD.partitions.length == expected))
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  test(GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered: left outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 15.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // In a left-outer join, and when the left side has larger stats, partially clustered
    // distribution should kick in and pick the right hand side to replicate partitions.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i LEFT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(
                  scans.forall(_.inputRDD.partitions.length == expected),
                  s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, null),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 15.0, null),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  test(GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered: right outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // The left-hand side is picked as the side to replicate partitions based on stats, but since
    // this is right outer join, partially clustered distribution won't kick in, and Spark should
    // only push down partition values on both side.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i RIGHT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.map(_.inputRDD.partitions.length).toSet.size == 1)
                assert(
                  scans.forall(_.inputRDD.partitions.length == expected),
                  s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST + "SPARK-42038: partially clustered:" +
      " full outer join is not applicable") {
    val items_partitions = Array(identity("id"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i FULL OUTER JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val scans = collectScans(df.queryExecution.executedPlan)
                assert(scans.map(_.inputRDD.partitions.length).toSet.size == 1)
                assert(
                  scans.forall(_.inputRDD.partitions.length == expected),
                  s"Expected $expected but got ${scans.head.inputRDD.partitions.length}")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 20.0),
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 15.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }
}
