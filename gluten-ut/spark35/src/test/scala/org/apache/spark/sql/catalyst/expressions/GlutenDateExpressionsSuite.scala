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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, TimeZoneUTC}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit._

class GlutenDateExpressionsSuite extends DateExpressionsSuite with GlutenTestsTrait {
  override def testIntegralInput(testFunc: Number => Unit): Unit = {
    def checkResult(input: Long): Unit = {
      if (input.toByte == input) {
        testFunc(input.toByte)
      } else if (input.toShort == input) {
        testFunc(input.toShort)
      } else if (input.toInt == input) {
        testFunc(input.toInt)
      } else {
        testFunc(input)
      }
    }

    checkResult(0)
    checkResult(Byte.MaxValue)
    checkResult(Byte.MinValue)
    checkResult(Short.MaxValue)
    checkResult(Short.MinValue)
    // Spark collect causes integer overflow.
    // checkResult(Int.MaxValue)
    // checkResult(Int.MinValue)
    // checkResult(Int.MaxValue.toLong + 100)
    // checkResult(Int.MinValue.toLong - 100)
  }

  test(GLUTEN_TEST + "TIMESTAMP_MICROS") {
    def testIntegralFunc(value: Number): Unit = {
      checkEvaluation(MicrosToTimestamp(Literal(value)), value.longValue())
    }

    // test null input
    checkEvaluation(MicrosToTimestamp(Literal(null, IntegerType)), null)

    // test integral input
    testIntegralInput(testIntegralFunc)
    // test max/min input
    // Spark collect causes long overflow.
    // testIntegralFunc(Long.MaxValue)
    // testIntegralFunc(Long.MinValue)
  }

  val outstandingTimezonesIds: Seq[String] = Seq(
    // Velox doesn't support timezones like UTC.
    // "UTC",
    PST.getId,
    CET.getId,
    "Africa/Dakar",
    LA.getId,
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels")
  val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(getZoneId)

  test(GLUTEN_TEST + "unix_timestamp") {
    Seq("legacy", "corrected").foreach {
      legacyParserPolicy =>
        withDefaultTimeZone(UTC) {
          for (zid <- outstandingZoneIds) {
            withSQLConf(
              SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
            ) {
              val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
              val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
              val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
              val fmt3 = "yy-MM-dd"
              val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
              sdf3.setTimeZone(TimeZoneUTC)

              val timeZoneId = Option(zid.getId)
              val tz = TimeZone.getTimeZone(zid)
              sdf1.setTimeZone(tz)
              sdf2.setTimeZone(tz)

              val date1 = Date.valueOf("2015-07-24")
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf1.format(new Timestamp(0))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                0L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf1.format(new Timestamp(1000000))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(new Timestamp(1000000)),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(
                    DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(1000000))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(Literal(date1), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), tz.toZoneId))
              )
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf2.format(new Timestamp(-1000000))),
                  Literal(fmt2),
                  timeZoneId),
                -1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf3.format(Date.valueOf("2015-07-24"))),
                  Literal(fmt3),
                  timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(
                    DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")),
                    tz.toZoneId))
              )
              val t1 = UnixTimestamp(CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss"))
                .eval()
                .asInstanceOf[Long]
              val t2 = UnixTimestamp(CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss"))
                .eval()
                .asInstanceOf[Long]
              assert(t2 - t1 <= 1)
              checkEvaluation(
                UnixTimestamp(
                  Literal.create(null, DateType),
                  Literal.create(null, StringType),
                  timeZoneId),
                null)
              checkEvaluation(
                UnixTimestamp(
                  Literal.create(null, DateType),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                null)
              checkEvaluation(
                UnixTimestamp(Literal(date1), Literal.create(null, StringType), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), tz.toZoneId))
              )
            }
          }
        }
    }
    // Test escaping of format
    GenerateUnsafeProjection.generate(
      UnixTimestamp(Literal("2015-07-24"), Literal("\""), UTC_OPT) :: Nil)
  }

  test(GLUTEN_TEST + "to_unix_timestamp") {
    withDefaultTimeZone(UTC) {
      for (zid <- outstandingZoneIds) {
        Seq("legacy", "corrected").foreach {
          legacyParserPolicy =>
            withSQLConf(
              SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
            ) {
              val fmt1 = "yyyy-MM-dd HH:mm:ss"
              val sdf1 = new SimpleDateFormat(fmt1, Locale.US)
              val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
              val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
              val fmt3 = "yy-MM-dd"
              val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
              sdf3.setTimeZone(TimeZoneUTC)

              val timeZoneId = Option(zid.getId)
              val tz = TimeZone.getTimeZone(zid)
              sdf1.setTimeZone(tz)
              sdf2.setTimeZone(tz)

              val date1 = Date.valueOf("2015-07-24")
              checkEvaluation(
                ToUnixTimestamp(Literal(sdf1.format(new Timestamp(0))), Literal(fmt1), timeZoneId),
                0L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf1.format(new Timestamp(1000000))),
                  Literal(fmt1),
                  timeZoneId),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(Literal(new Timestamp(1000000)), Literal(fmt1)),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(
                    DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(1000000))),
                  Literal(fmt1)),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(Literal(date1), Literal(fmt1), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), zid)))
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf2.format(new Timestamp(-1000000))),
                  Literal(fmt2),
                  timeZoneId),
                -1000L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf3.format(Date.valueOf("2015-07-24"))),
                  Literal(fmt3),
                  timeZoneId),
                MICROSECONDS.toSeconds(DateTimeUtils
                  .daysToMicros(DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")), zid))
              )
              val t1 = ToUnixTimestamp(CurrentTimestamp(), Literal(fmt1)).eval().asInstanceOf[Long]
              val t2 = ToUnixTimestamp(CurrentTimestamp(), Literal(fmt1)).eval().asInstanceOf[Long]
              assert(t2 - t1 <= 1)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal.create(null, DateType),
                  Literal.create(null, StringType),
                  timeZoneId),
                null)
              checkEvaluation(
                ToUnixTimestamp(Literal.create(null, DateType), Literal(fmt1), timeZoneId),
                null)
              checkEvaluation(
                ToUnixTimestamp(Literal(date1), Literal.create(null, StringType), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), zid))
              )

              // SPARK-28072 The codegen path for non-literal input should also work
              checkEvaluation(
                expression = ToUnixTimestamp(
                  BoundReference(ordinal = 0, dataType = StringType, nullable = true),
                  BoundReference(ordinal = 1, dataType = StringType, nullable = true),
                  timeZoneId),
                expected = 0L,
                inputRow = InternalRow(
                  UTF8String.fromString(sdf1.format(new Timestamp(0))),
                  UTF8String.fromString(fmt1))
              )
            }
        }
      }
    }
    // Test escaping of format
    GenerateUnsafeProjection.generate(
      ToUnixTimestamp(Literal("2015-07-24"), Literal("\""), UTC_OPT) :: Nil)
  }

  // Modified based on vanilla spark to explicitly set timezone in config.
  test(GLUTEN_TEST + "DateFormat") {
    val PST_OPT = Option("America/Los_Angeles")
    val JST_OPT = Option("Asia/Tokyo")

    Seq("legacy", "corrected").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC_OPT.get) {
          checkEvaluation(
            DateFormatClass(Literal.create(null, TimestampType), Literal("y"), UTC_OPT),
            null)
          checkEvaluation(
            DateFormatClass(
              Cast(Literal(d), TimestampType, UTC_OPT),
              Literal.create(null, StringType),
              UTC_OPT),
            null)

          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, UTC_OPT), Literal("y"), UTC_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), UTC_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, UTC_OPT), Literal("H"), UTC_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), UTC_OPT), "13")
        }

        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> PST_OPT.get) {
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, PST_OPT), Literal("y"), PST_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), PST_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, PST_OPT), Literal("H"), PST_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), PST_OPT), "5")
        }

        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> JST_OPT.get) {
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, JST_OPT), Literal("y"), JST_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), JST_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, JST_OPT), Literal("H"), JST_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), JST_OPT), "22")
        }
    }
  }
}
