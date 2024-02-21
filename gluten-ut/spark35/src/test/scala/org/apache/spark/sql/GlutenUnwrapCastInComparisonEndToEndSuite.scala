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

import scala.collection.immutable.Seq

class GlutenUnwrapCastInComparisonEndToEndSuite
  extends UnwrapCastInComparisonEndToEndSuite
  with GlutenSQLTestsTrait {

  import testImplicits._

  test(GLUTEN_TEST + "cases when literal is max") {
    withTable(t) {
      Seq[(Integer, java.lang.Short, java.lang.Float)](
        (1, 100.toShort, 3.14.toFloat),
        (2, Short.MaxValue, Float.NaN),
        (3, Short.MinValue, Float.PositiveInfinity),
        (4, 0.toShort, Float.MaxValue),
        (5, null, null))
        .toDF("c1", "c2", "c3")
        .write
        .saveAsTable(t)
      val df = spark.table(t)

      val lit = Short.MaxValue.toInt
      checkAnswer(df.where(s"c2 > $lit").select("c1"), Seq.empty)
      checkAnswer(df.where(s"c2 >= $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 == $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 <=> $lit").select("c1"), Row(2))
      checkAnswer(df.where(s"c2 != $lit").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c2 <= $lit").select("c1"), Row(1) :: Row(2) :: Row(3) :: Row(4) :: Nil)
      checkAnswer(df.where(s"c2 < $lit").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)

      // NaN is not supported in velox, so unexpected result will be obtained.
//      checkAnswer(df.where(s"c3 > double('nan')").select("c1"), Seq.empty)
//      checkAnswer(df.where(s"c3 >= double('nan')").select("c1"), Row(2))
//      checkAnswer(df.where(s"c3 == double('nan')").select("c1"), Row(2))
//      checkAnswer(df.where(s"c3 <=> double('nan')").select("c1"), Row(2))
//    checkAnswer(df.where(s"c3 != double('nan')").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
//      checkAnswer(df.where(s"c3 <= double('nan')").select("c1"),
//        Row(1) :: Row(2) :: Row(3) :: Row(4) :: Nil)
//      checkAnswer(df.where(s"c3 < double('nan')").select("c1"), Row(1) :: Row(3) :: Row(4) :: Nil)
    }
  }
}
