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
import org.apache.gluten.execution.SortMergeJoinExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Collections

class GlutenKeyGroupedPartitioningSuite
  extends KeyGroupedPartitioningSuite
  with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "false")
      .set("spark.sql.adaptive.enabled", "false")
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
      None)
  }

  private val customers: String = "customers"
  private val customers_schema = new StructType()
    .add("customer_name", StringType)
    .add("customer_age", IntegerType)
    .add("customer_id", LongType)

  private val orders: String = "orders"
  private val orders_schema = new StructType()
    .add("order_amount", DoubleType)
    .add("customer_id", LongType)

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int): Unit = {
    createTable(customers, customers_schema, customers_partitions)
    sql(
      s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, orders_schema, orders_partitions)
    sql(
      s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql(
      "SELECT customer_name, customer_age, order_amount " +
        s"FROM testcat.ns.$customers c JOIN testcat.ns.$orders o " +
        "ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.length == expectedNumOfShuffleExecs)

    checkAnswer(
      df,
      Seq(
        Row("aaa", 10, 100.0),
        Row("aaa", 10, 200.0),
        Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0),
        Row("bbb", 20, 350.0),
        Row("ccc", 30, 400.50)))
  }

  private def collectColumnarShuffleExchangeExec(
      plan: SparkPlan): Seq[ColumnarShuffleExchangeExec] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }.flatMap(smj => collect(smj) { case s: ColumnarShuffleExchangeExec => s })
  }

  testGluten("partitioned join: only one side reports partitioning") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(2, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2)
  }
  testGluten("partitioned join: exact distribution (same number of buckets) from both sides") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 0)
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

  testGluten("partitioned join:  join with two partition keys and matching & sorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and unsorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and different # of partition keys") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, items_schema, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchases_schema, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.nonEmpty, "should add shuffle when partition keys mismatch")
  }

  testGluten("data source partitioning + dynamic partition filtering") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10"
    ) {
      val items_partitions = Array(identity("id"))
      createTable(items, items_schema, items_partitions)
      sql(
        s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchases_schema, purchases_partitions)
      sql(
        s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      // number of unique partitions changed after dynamic filtering - should throw exception
      var df = sql(
        s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p WHERE " +
          s"i.id = p.item_id AND i.price > 40.0")
      val e = intercept[Exception](df.collect())
      assert(e.getMessage.contains("number of unique partition values"))

      // dynamic filtering doesn't change partitioning so storage-partitioned join should kick in
      df = sql(
        s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p WHERE " +
          s"i.id = p.item_id AND i.price >= 10.0")
      val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
      assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
      checkAnswer(df, Seq(Row(303.5)))
    }
  }
}
