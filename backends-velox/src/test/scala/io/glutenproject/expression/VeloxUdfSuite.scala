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
package io.glutenproject.expression

import io.glutenproject.tags.{SkipTestTags, UDFTest}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper

import java.nio.file.Paths
import java.sql.Date

abstract class VeloxUdfSuite extends GlutenQueryTest with SQLHelper {

  protected val master: String

  private var _spark: SparkSession = _

  // This property is used for unit tests.
  val UDFLibPathProperty: String = "velox.udf.lib.path"

  protected lazy val udfLibPath: String =
    sys.props.get(UDFLibPathProperty) match {
      case Some(path) =>
        path
      case None =>
        throw new IllegalArgumentException(
          UDFLibPathProperty + s" cannot be null. You may set it by adding " +
            s"-D$UDFLibPathProperty=" +
            "/path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so")
    }

  protected lazy val udfLibRelativePath: String =
    udfLibPath.split(",").map(p => Paths.get(p).getFileName.toString).mkString(",")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .master(master)
        .config(sparkConf)
        .getOrCreate()
    }

    _spark.sparkContext.setLogLevel("info")
  }

  override protected def spark = _spark

  protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  test("test udf") {
    val df = spark.sql("""select
                         |  myudf1(1),
                         |  myudf1(1L),
                         |  myudf2(100L),
                         |  mydate(cast('2024-03-25' as date), 5),
                         |  myavg(1)
                         |""".stripMargin)
    df.collect()
    assert(df.collect().sameElements(Array(Row(6, 6L, 105, Date.valueOf("2024-03-30"), 1.0))))
  }
}

@UDFTest
class VeloxUdfSuiteLocal extends VeloxUdfSuite {

  override val master: String = "local[2]"
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set("spark.gluten.sql.columnar.backend.velox.udfLibraryPaths", udfLibRelativePath)
  }
}

// Set below environment variables and VM options to run this test:
// export SCALA_HOME=/usr/share/scala
// export SPARK_SCALA_VERSION=2.12
//
// VM options:
// -Dspark.test.home=${SPARK_HOME}
// -Dgluten.package.jar=\
// /path/to/gluten/package/target/gluten-package-${project.version}.jar
// -Dvelox.udf.lib.path=\
// /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
@SkipTestTags
class VeloxUdfSuiteCluster extends VeloxUdfSuite {

  override val master: String = "local-cluster[2,2,1024]"

  val GLUTEN_JAR: String = "gluten.package.jar"

  private lazy val glutenJar = sys.props.get(GLUTEN_JAR) match {
    case Some(jar) => jar
    case None =>
      throw new IllegalArgumentException(
        GLUTEN_JAR + s" cannot be null. You may set it by adding " +
          s"-D$GLUTEN_JAR=" +
          "/path/to/gluten/package/target/gluten-package-${project.version}.jar")
  }

  private lazy val driverUdfLibPath =
    udfLibPath.split(",").map("file://" + _).mkString(",")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set("spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths", driverUdfLibPath)
      .set("spark.gluten.sql.columnar.backend.velox.udfLibraryPaths", udfLibRelativePath)
      .set("spark.driver.extraClassPath", glutenJar)
      .set("spark.executor.extraClassPath", glutenJar)
  }
}
