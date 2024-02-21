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
package org.apache.spark.sql

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import java.sql.{Date, Timestamp}
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit

class GlutenDateFunctionsSuite extends DateFunctionsSuite with GlutenSQLTestsTrait {
  import testImplicits._

  private def secs(millis: Long): Long = TimeUnit.MILLISECONDS.toSeconds(millis)

  test(GLUTEN_TEST + "unix_timestamp") {
    Seq("corrected", "legacy").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> ZoneId.systemDefault().toString) {
          val date1 = Date.valueOf("2015-07-24")
          val date2 = Date.valueOf("2015-07-25")
          val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
          val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
          val ntzTs1 = LocalDateTime.parse("2015-07-24T10:00:00.3")
          val ntzTs2 = LocalDateTime.parse("2015-07-25T02:02:02.2")
          val s1 = "2015/07/24 10:00:00.5"
          val s2 = "2015/07/25 02:02:02.6"
          val ss1 = "2015-07-24 10:00:00"
          val ss2 = "2015-07-25 02:02:02"
          val fmt = "yyyy/MM/dd HH:mm:ss.S"
          val df = Seq((date1, ts1, ntzTs1, s1, ss1), (date2, ts2, ntzTs2, s2, ss2)).toDF(
            "d",
            "ts",
            "ntzTs",
            "s",
            "ss")
          checkAnswer(
            df.select(unix_timestamp(col("ts"))),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("ss"))),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("ntzTs"))),
            Seq(
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))
            )
          )
          checkAnswer(
            df.select(unix_timestamp(col("d"), fmt)),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("s"), fmt)),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ts)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ss)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ntzTs)"),
            Seq(
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))
            )
          )
          checkAnswer(
            df.selectExpr(s"unix_timestamp(d, '$fmt')"),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.selectExpr(s"unix_timestamp(s, '$fmt')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val x1 = "2015-07-24 10:00:00"
          val x2 = "2015-25-07 02:02:02"
          val x3 = "2015-07-24 25:02:02"
          val x4 = "2015-24-07 26:02:02"
          val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
          val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

          val df1 = Seq(x1, x2, x3, x4).toDF("x")
          checkAnswer(
            df1.select(unix_timestamp(col("x"))),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr("unix_timestamp(x)"),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.select(unix_timestamp(col("x"), "yyyy-dd-MM HH:mm:ss")),
            Seq(Row(null), Row(secs(ts2.getTime)), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"),
            Seq(Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

          // legacyParserPolicy is not respected by Gluten.
          // invalid format
          // val invalid = df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd aa:HH:ss')")
          // if (legacyParserPolicy == "legacy") {
          //   checkAnswer(invalid,
          //     Seq(Row(null), Row(null), Row(null), Row(null)))
          // } else {
          //   val e = intercept[SparkUpgradeException](invalid.collect())
          //   assert(e.getCause.isInstanceOf[IllegalArgumentException])
          //   assert( e.getMessage.contains(
          //     "You may get a different result due to the upgrading to Spark"))
          // }

          // February
          val y1 = "2016-02-29"
          val y2 = "2017-02-29"
          val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
          val df2 = Seq(y1, y2).toDF("y")
          checkAnswer(
            df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")),
            Seq(Row(secs(ts5.getTime)), Row(null)))

          val now = sql("select unix_timestamp()").collect().head.getLong(0)
          checkAnswer(
            sql(s"select timestamp_seconds($now)"),
            Row(new java.util.Date(TimeUnit.SECONDS.toMillis(now))))
        }
    }
  }

  test(GLUTEN_TEST + "to_unix_timestamp") {
    Seq("corrected", "legacy").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> ZoneId.systemDefault().toString
        ) {
          val date1 = Date.valueOf("2015-07-24")
          val date2 = Date.valueOf("2015-07-25")
          val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
          val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
          val s1 = "2015/07/24 10:00:00.5"
          val s2 = "2015/07/25 02:02:02.6"
          val ss1 = "2015-07-24 10:00:00"
          val ss2 = "2015-07-25 02:02:02"
          val fmt = "yyyy/MM/dd HH:mm:ss.S"
          val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ts)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ss)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr(s"to_unix_timestamp(d, '$fmt')"),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.selectExpr(s"to_unix_timestamp(s, '$fmt')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val x1 = "2015-07-24 10:00:00"
          val x2 = "2015-25-07 02:02:02"
          val x3 = "2015-07-24 25:02:02"
          val x4 = "2015-24-07 26:02:02"
          val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
          val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

          val df1 = Seq(x1, x2, x3, x4).toDF("x")
          checkAnswer(
            df1.selectExpr("to_unix_timestamp(x)"),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"),
            Seq(Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

          // February
          val y1 = "2016-02-29"
          val y2 = "2017-02-29"
          val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
          val df2 = Seq(y1, y2).toDF("y")
          checkAnswer(
            df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")),
            Seq(Row(secs(ts5.getTime)), Row(null)))

          // Not consistent behavior with gluten + velox.
          // invalid format
          //        val invalid = df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd bb:HH:ss')")
          //        val e = intercept[IllegalArgumentException](invalid.collect())
          //        assert(e.getMessage.contains('b'))
        }
    }
  }
}
