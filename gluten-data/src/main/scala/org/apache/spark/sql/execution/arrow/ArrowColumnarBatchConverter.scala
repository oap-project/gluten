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
package org.apache.spark.sql.execution.arrow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._

object ArrowColumnarBatchConverter {

  def create(arrowSchema: Schema, bufferAllocator: BufferAllocator): ArrowColumnarBatchConverter = {
    val root = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
    create(root)
  }

  def create(root: VectorSchemaRoot): ArrowColumnarBatchConverter = {
    val children = root.getFieldVectors.asScala.map {
      vector =>
        vector.allocateNew()
        createFieldWriter(vector)
    }
    new ArrowColumnarBatchConverter(root, children.toArray)
  }

  private def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    (ArrowUtils.fromArrowField(vector.getField), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector)
        new ArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new MapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map {
          ordinal => createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (NullType, vector: NullVector) => new NullWriter(vector)
      // TODO support YearMonthIntervalType, DayTimeIntervalType
      case (dt, _) =>
        throw new UnsupportedOperationException("Unsupported data type: " + dt)
    }
  }
}

case class ColumnarSpecializedGetters(columnVector: ColumnVector) extends SpecializedGetters {

  override def isNullAt(rowId: Int): Boolean = columnVector.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = columnVector.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = columnVector.getByte(rowId)

  override def getShort(rowId: Int): Short = columnVector.getShort(rowId)

  override def getInt(rowId: Int): Int = columnVector.getInt(rowId)

  override def getLong(rowId: Int): Long = columnVector.getLong(rowId)

  override def getFloat(rowId: Int): Float = columnVector.getFloat(rowId)

  override def getDouble(rowId: Int): Double = columnVector.getDouble(rowId)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    columnVector.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = columnVector.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = columnVector.getBinary(rowId)

  override def getInterval(rowId: Int): CalendarInterval = columnVector.getInterval(rowId)

  override def getStruct(rowId: Int, numFields: Int): InternalRow = columnVector.getStruct(rowId)

  override def getArray(rowId: Int): ArrayData = columnVector.getArray(rowId)

  override def getMap(rowId: Int): MapData = columnVector.getMap(rowId)

  override def get(rowId: Int, dataType: DataType): AnyRef = {
    throw new UnsupportedOperationException("Not implemented yet")
  }
}

class ArrowColumnarBatchConverter(val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {

  private var count: Int = 0

  def write(columnarBatch: ColumnarBatch): Unit = {
    fields.zipWithIndex.foreach {
      case (field, ordinal) =>
        val columnVector = ColumnarSpecializedGetters(columnarBatch.column(ordinal))
        for (rowId <- 0 until columnarBatch.numRows()) {
          field.write(columnVector, rowId)
        }
    }
    count += columnarBatch.numRows()
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }

  def close(): Unit = {
    root.close()
  }
}
