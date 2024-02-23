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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

class VeloxStringFunctionsSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  final val NULL_STR_COL: String = "nullStringColumn"
  final val LINEITEM_TABLE: String = "lineitem_nullStringColumn"
  final val LENGTH = 1000

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
    spark
      .table("lineitem")
      .select(col("*"), new Column(Alias(Literal(null, StringType), NULL_STR_COL)()))
      .createOrReplaceTempView(LINEITEM_TABLE)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
  }

  test("ascii") {
    runQueryAndCompare(
      s"select l_orderkey, ascii(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      s"select l_orderkey, ascii($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("concat") {
    runQueryAndCompare(
      s"select l_orderkey, concat(l_comment, 'hello') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, concat(l_comment, 'hello', 'world') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("extract") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, " +
        s"extract(doy FROM DATE'2019-08-12') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("day") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, day(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, day($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("dayofmonth") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, dayofmonth(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, dayofmonth($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("day_of_year") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, dayofyear(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, dayofyear($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("dayofweek") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, dayofweek(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, dayofweek($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  ignore("weekday") { // todo: result mismatched
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, weekday(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, weekday($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("month") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, month(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, month($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("quarter") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, quarter(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, quarter($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("year") {
    runQueryAndCompare(
      s"select l_orderkey, l_shipdate, year(l_shipdate) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, year($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("instr") {
    runQueryAndCompare(
      s"select l_orderkey, instr(l_comment, 'h') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, instr(l_comment, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, instr($NULL_STR_COL, 'h') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("length") {
    runQueryAndCompare(
      s"select l_orderkey, length(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, length($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      s"select l_orderkey, CHAR_LENGTH(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, CHAR_LENGTH($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      s"select l_orderkey, CHARACTER_LENGTH(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, CHARACTER_LENGTH($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("md5") {
    runQueryAndCompare(
      s"select l_orderkey, md5(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, md5($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("sha1") {
    runQueryAndCompare(
      s"select l_orderkey, sha1(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, sha1($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("sha2") {
    Seq(-1, 0, 1, 224, 256, 384, 512).foreach {
      bitLength =>
        runQueryAndCompare(
          s"select l_orderkey, sha2(l_comment, $bitLength) " +
            s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    }
    runQueryAndCompare(
      s"select l_orderkey, sha2($NULL_STR_COL, 256) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, sha2(l_comment, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("crc32") {
    runQueryAndCompare(
      s"select l_orderkey, crc32(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, crc32($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("lower") {
    runQueryAndCompare(
      s"select l_orderkey, lower(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lower($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("upper") {
    runQueryAndCompare(
      s"select l_orderkey, upper(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, upper($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("lcase") {
    runQueryAndCompare(
      s"select l_orderkey, lcase(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lcase($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("ucase") {
    runQueryAndCompare(
      s"select l_orderkey, ucase(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, ucase($NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  ignore("locate") {
    runQueryAndCompare(
      s"select l_orderkey, locate(l_comment, 'a', 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, locate($NULL_STR_COL, 'a', 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("trim") {
    runQueryAndCompare(
      s"select l_orderkey, trim(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, trim('. abcdefg', l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, trim($NULL_STR_COL), " +
        s"trim($NULL_STR_COL, l_comment), trim('. abcdefg', $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("ltrim") {
    runQueryAndCompare(
      s"select l_orderkey, ltrim(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, ltrim('. abcdefg', l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, ltrim($NULL_STR_COL), " +
        s"ltrim($NULL_STR_COL, l_comment), ltrim('. abcdefg', $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("rtrim") {
    runQueryAndCompare(
      s"select l_orderkey, rtrim(l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rtrim('. abcdefg', l_comment) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rtrim($NULL_STR_COL), " +
        s"rtrim($NULL_STR_COL, l_comment), rtrim('. abcdefg', $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("lpad") {
    runQueryAndCompare(
      s"select l_orderkey, lpad($NULL_STR_COL, 80) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80, '??') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, $NULL_STR_COL, '??') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, lpad(l_comment, 80, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("rpad") {
    runQueryAndCompare(
      s"select l_orderkey, rpad($NULL_STR_COL, 80) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80, '??') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, $NULL_STR_COL, '??') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rpad(l_comment, 80, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("like") {
    runQueryAndCompare(
      """select l_orderkey, like(l_comment, '%\%') """ +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, 'a_%b') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, 'a\\__b') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, 'abc_') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, ' ') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, '%a%') " +
        s"from $LINEITEM_TABLE where l_comment like '%a%' limit $LENGTH") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, ' ') " +
        s"from $LINEITEM_TABLE where l_comment like ''  limit $LENGTH") { _ => }
    runQueryAndCompare(
      s"select l_orderkey, like($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE where l_comment like '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("rlike") {
    runQueryAndCompare(
      s"select l_orderkey, l_comment, rlike(l_comment, 'a*') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rlike(l_comment, ' ') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, rlike($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, l_comment " +
        s"from $LINEITEM_TABLE where l_comment rlike '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(
      s"select l_orderkey, like(l_comment, ' ') " +
        s"from $LINEITEM_TABLE where l_comment rlike ''  limit $LENGTH") { _ => }
    runQueryAndCompare(
      s"select l_orderkey, like($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE where l_comment rlike '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp") {
    runQueryAndCompare(
      s"select l_orderkey, l_comment, regexp(l_comment, 'a*') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp(l_comment, ' ') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, l_comment " +
        s"from $LINEITEM_TABLE where l_comment regexp '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(
      s"select l_orderkey, l_comment " +
        s"from $LINEITEM_TABLE where l_comment regexp ''  limit $LENGTH") { _ => }
    runQueryAndCompare(
      s"select l_orderkey, l_comment " +
        s"from $LINEITEM_TABLE where l_comment regexp '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp_like") {
    runQueryAndCompare(
      s"select l_orderkey, l_comment, regexp_like(l_comment, 'a*') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_like(l_comment, ' ') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_like($NULL_STR_COL, '%a%') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("regexp_extract") {
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract($NULL_STR_COL, '([a-z])', 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("regexp_extract_all") {
    runQueryAndCompare(
      s"select l_orderkey, regexp_extract_all(l_comment, '([a-z])', 1) " +
        s"from $LINEITEM_TABLE limit 5")(checkOperatorMatch[ProjectExecTransformer])
    // fall back because of unsupported cast(array)
    runQueryAndCompare(
      s"select l_orderkey, l_comment, " +
        s"regexp_extract_all(l_comment, '([a-z]+)', 0) " +
        s"from $LINEITEM_TABLE limit 5") { _ => }
  }

  test("regexp_replace") {
    runQueryAndCompare(
      s"select l_orderkey, regexp_replace(l_comment, '([a-z])', '1') " +
        s"from $LINEITEM_TABLE limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 1) " +
        s"from $LINEITEM_TABLE limit 5")(checkOperatorMatch[ProjectExecTransformer])
    // todo incorrect results
    runQueryAndCompare(
      s"select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 10) " +
        s"from $LINEITEM_TABLE limit 5",
      true,
      false)(_ => {})
  }

  test("regex invalid") {
    // Positive lookahead
    runQueryAndCompare(
      s"""select regexp_replace(l_returnflag, "(?=N)", "Y") from $LINEITEM_TABLE limit 5""",
      true,
      false)(_ => {})
    // Negative lookahead
    runQueryAndCompare(
      s"""select regexp_replace(l_returnflag, "(?!N)", "Y") from $LINEITEM_TABLE limit 5""",
      true,
      false)(_ => {})
    // Positive lookbehind
    runQueryAndCompare(
      s"""select rlike(l_returnflag, "(?<=N)") from $LINEITEM_TABLE limit 5""",
      true,
      false)(_ => {})
    // Negative lookbehind
    runQueryAndCompare(
      s"""select rlike(l_returnflag, "(?<!N)") from $LINEITEM_TABLE limit 5""",
      true,
      false)(_ => {})
  }

  test("replace") {
    runQueryAndCompare(
      s"select l_orderkey, replace(l_comment, ' ', 'hello') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, replace(l_comment, 'ha') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, replace(l_comment, ' ', $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, replace(l_comment, $NULL_STR_COL, 'hello') " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("reverse") {
    runQueryAndCompare(
      s"select l_orderkey, l_comment, reverse(l_comment) " +
        s"from $LINEITEM_TABLE limit 5")(checkOperatorMatch[ProjectExecTransformer])

    // fall back because of unsupported cast(array)
    runQueryAndCompare(
      s"select l_orderkey, l_comment, reverse(array(l_comment, l_comment)) " +
        s"from $LINEITEM_TABLE limit 5") { _ => }
  }

  test("split") {
    runQueryAndCompare(
      s"select l_orderkey, l_comment, split(l_comment, ' ', 3) " +
        s"from $LINEITEM_TABLE limit 5") { _ => }

    runQueryAndCompare(
      s"select l_orderkey, l_comment, split(l_comment, '[a]', 3) " +
        s"from $LINEITEM_TABLE limit 5") { _ => }

    runQueryAndCompare(
      s"select l_orderkey, split(l_comment, ' ') " +
        s"from $LINEITEM_TABLE limit 5") { _ => }

    runQueryAndCompare(
      s"select l_orderkey, split(l_comment, 'h') " +
        s"from $LINEITEM_TABLE limit 5") { _ => }
  }

  test("substr") {
    runQueryAndCompare(
      s"select l_orderkey, substr(l_comment, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substr(l_comment, 1, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substr($NULL_STR_COL, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substr($NULL_STR_COL, 1, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substr(l_comment, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substr(l_comment, $NULL_STR_COL, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("substring") {
    runQueryAndCompare(
      s"select l_orderkey, substring(l_comment, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substring(l_comment, 1, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substring($NULL_STR_COL, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substring($NULL_STR_COL, 1, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substring(l_comment, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, substring(l_comment, $NULL_STR_COL, 3) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("left") {
    runQueryAndCompare(
      s"select l_orderkey, left(l_comment, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      s"select l_orderkey, left($NULL_STR_COL, 1) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      s"select l_orderkey, left(l_comment, $NULL_STR_COL) " +
        s"from $LINEITEM_TABLE limit $LENGTH")(checkOperatorMatch[ProjectExecTransformer])
  }
}
