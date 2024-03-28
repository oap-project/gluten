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
package org.apache.gluten.benchmarks

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import com.github.javafaker.Faker

import java.sql.Date
import java.util.Random

case class RandomParquetDataGenerator(initialSeed: Long = 0L) {
  private var seed: Long = initialSeed
  private var faker = new Faker(new Random(seed))

  def reFake(newSeed: Long): Unit = {
    seed = newSeed
    faker = new Faker(new Random(seed))
  }

  def getSeed: Long = {
    seed
  }

  def getFaker: Faker = {
    faker
  }

  def generateRow(schema: StructType, probabilityOfNull: Double = 0): Row = {
    val values = schema.fields.map(field => generateDataForType(field.dataType, probabilityOfNull))
    Row.fromSeq(values)
  }

  def generateDataForType(dataType: DataType, probabilityOfNull: Double): Any = {
    require(
      probabilityOfNull >= 0 && probabilityOfNull <= 1,
      "Probability should be between 0 and 1")

    if (faker.random().nextDouble() < probabilityOfNull) {
      return null
    }

    dataType match {
      case BooleanType => faker.bool().bool()
      case ByteType => faker.number().numberBetween(Byte.MinValue, Byte.MaxValue).toByte
      case ShortType => faker.number().numberBetween(Short.MinValue, Short.MaxValue).toShort
      case IntegerType => faker.number().numberBetween(Int.MinValue, Int.MaxValue)
      case LongType => faker.number().numberBetween(Long.MinValue, Long.MaxValue)
      case FloatType =>
        faker.number().randomDouble(2, Float.MinValue.toInt, Float.MaxValue.toInt).toFloat
      case DoubleType =>
        faker.number().randomDouble(2, Double.MinValue.toLong, Double.MaxValue.toLong)
      case DateType => new Date(faker.date().birthday().getTime)
//      case TimestampType => new Timestamp(faker.date().birthday().getTime)
      case t: DecimalType =>
        BigDecimal(
          faker.number().randomDouble(t.scale, 0, Math.pow(10, t.precision - t.scale).toLong))
      case StringType => faker.lorem().characters(0, 1000)
      case BinaryType => faker.lorem().characters(10).getBytes
      case ArrayType(elementType, _) =>
        Seq.fill(faker.number().numberBetween(1, 5))(
          generateDataForType(elementType, probabilityOfNull))
      case MapType(keyType, valueType, _) =>
        Map(generateDataForType(keyType, 0) -> generateDataForType(valueType, probabilityOfNull))
      case struct: StructType => generateRow(struct)
      case _ =>
        throw new UnsupportedOperationException(
          s"Data generation not supported for type: $dataType")
    }
  }

  def generateRandomData(
      spark: SparkSession,
      schema: StructType,
      numRows: Int,
      outputPath: String): Unit = {
    val data = (0 until numRows).map(_ => generateRow(schema, faker.random().nextDouble()))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputPath)
  }

  def generateRandomData(spark: SparkSession, outputPath: String): Unit = {
    val schema = generateRandomSchema()
    val numRows = faker.random().nextInt(1000, 300000)
    generateRandomData(spark, schema, numRows, outputPath)
  }

  var fieldIndex = 0
  def fieldName: String = {
    fieldIndex += 1
    s"f_$fieldIndex"
  }

  // Candidate fields
  val numericFields: List[() => StructField] = List(
    () => StructField(fieldName, BooleanType, nullable = true),
    () => StructField(fieldName, ByteType, nullable = true),
    () => StructField(fieldName, ShortType, nullable = true),
    () => StructField(fieldName, IntegerType, nullable = true),
    () => StructField(fieldName, LongType, nullable = true),
    () => StructField(fieldName, FloatType, nullable = true),
    () => StructField(fieldName, DoubleType, nullable = true),
    () => StructField(fieldName, DateType, nullable = true),
//    () => StructField(fieldName, TimestampType, nullable = true),
    () => StructField(fieldName, DecimalType(10, 2), nullable = true)
  )

  val binaryFields: List[() => StructField] = List(
    () => StructField(fieldName, StringType, nullable = true),
    () => StructField(fieldName, BinaryType, nullable = true)
  )

  val complexFields: List[() => StructField] = List(
    () => StructField(fieldName, ArrayType(StringType, containsNull = true), nullable = true),
    () =>
      StructField(
        fieldName,
        MapType(StringType, IntegerType, valueContainsNull = true),
        nullable = true),
    () =>
      StructField(
        fieldName,
        StructType(
          Seq(
            StructField(fieldName, StringType, nullable = true),
            StructField(fieldName, DoubleType, nullable = true)
          )),
        nullable = true)
  )

  val candidateFields: List[() => StructField] =
    numericFields ++ binaryFields ++ complexFields

  // Function to generate random schema with n fields
  def generateRandomSchema(n: Int): StructType = {
    fieldIndex = 0
    val selectedFields = {
      (0 until 3).map(_ => numericFields(faker.random().nextInt(numericFields.length))()) ++
        (0 until 3).map(_ => binaryFields(faker.random().nextInt(binaryFields.length))()) ++
        (0 until Math.max(0, n - 6))
          .map(_ => candidateFields(faker.random().nextInt(candidateFields.length))())
    }
    StructType(selectedFields)
  }

  // Generate random schema with [10, 30) fields
  def generateRandomSchema(): StructType = {
    generateRandomSchema(faker.random().nextInt(4, 24))
  }
}

// An example to demonstrate how to use RandomParquetDataGenerator to generate input data.
object RandomParquetDataGenerator {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local[1]").appName("Random Data Generator").getOrCreate()

    val seed: Long = 0L
    val outputPath = s"${seed}_output.parquet"

    RandomParquetDataGenerator(seed).generateRandomData(spark, outputPath)
  }
}
