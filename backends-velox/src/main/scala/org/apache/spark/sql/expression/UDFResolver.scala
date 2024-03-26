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
package org.apache.spark.sql.expression

import io.glutenproject.backendsapi.velox.BackendSettings
import io.glutenproject.exception.GlutenException
import io.glutenproject.expression.{ConverterUtils, ExpressionTransformer, ExpressionType, Transformable}
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.udf.UdfJniWrapper
import io.glutenproject.vectorized.JniWorkspace

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFiles}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

import com.google.common.collect.Lists

import java.io.File
import java.net.URI
import java.nio.file.{Files, FileVisitOption, Paths}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

case class UserDefinedAggregateFunction(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression],
    override val aggBufferAttributes: Seq[AttributeReference])
  extends AggregateFunction {

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }
}

case class UDFExpression(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression])
  extends Transformable {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }

  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer = {
    if (childrenTransformers.size != children.size) {
      throw new IllegalStateException(
        this.getClass.getSimpleName +
          ": getTransformer called before children transformer initialized.")
    }
    (args: Object) => {
      val transformers = childrenTransformers.map(_.doTransform(args))
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
      val functionId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName(name, children.map(_.dataType), FunctionConfig.REQ))

      val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
      ExpressionBuilder.makeScalarFunction(
        functionId,
        Lists.newArrayList(transformers: _*),
        typeNode)
    }
  }
}

object UDFResolver extends Logging {
  private val UDFNames = mutable.HashSet[String]()
  private val UDFMap = mutable.HashMap[(String, Seq[DataType]), ExpressionType]()

  private val UDAFNames = mutable.HashSet[String]()
  private val UDAFMap =
    mutable.HashMap[(String, Seq[DataType]), (ExpressionType, Seq[AttributeReference])]()

  private val LIB_EXTENSION = ".so"

  private lazy val isDriver: Boolean =
    "driver".equals(SparkEnv.get.executorId)

  // Called by JNI.
  def registerUDF(name: String, returnType: Array[Byte], argTypes: Array[Byte]): Unit = {
    registerUDF(
      name,
      ConverterUtils.parseFromBytes(returnType),
      ConverterUtils.parseFromBytes(argTypes))
  }

  private def registerUDF(
      name: String,
      returnType: ExpressionType,
      argTypes: ExpressionType): Unit = {
    assert(argTypes.dataType.isInstanceOf[StructType])
    UDFMap.put(
      (name, argTypes.dataType.asInstanceOf[StructType].fields.map(_.dataType)),
      returnType)
    UDFNames += name
    logInfo(s"Registered UDF: $name($argTypes) -> $returnType")
  }

  def registerUDAF(
      name: String,
      returnType: Array[Byte],
      argTypes: Array[Byte],
      intermediateTypes: Array[Byte]): Unit = {
    registerUDAF(
      name,
      ConverterUtils.parseFromBytes(returnType),
      ConverterUtils.parseFromBytes(argTypes),
      ConverterUtils.parseFromBytes(intermediateTypes)
    )
  }

  private def registerUDAF(
      name: String,
      returnType: ExpressionType,
      argTypes: ExpressionType,
      intermediateTypes: ExpressionType): Unit = {
    assert(argTypes.dataType.isInstanceOf[StructType])
    assert(intermediateTypes.dataType.isInstanceOf[StructType])

    val aggBufferAttributes =
      intermediateTypes.dataType.asInstanceOf[StructType].fields.zipWithIndex.map {
        case (f, index) =>
          AttributeReference(s"inter_$index", f.dataType, f.nullable)()
      }
    UDAFMap.put(
      (name, argTypes.dataType.asInstanceOf[StructType].fields.map(_.dataType)),
      (returnType, aggBufferAttributes)
    )
    UDAFNames += name
    logInfo(s"Registered UDAF: $name($argTypes) -> $returnType")
  }

  def parseName(name: String): (String, String) = {
    val index = name.lastIndexOf("#")
    if (index == -1) {
      (name, Paths.get(name).getFileName.toString)
    } else {
      (name.substring(0, index), name.substring(index + 1))
    }
  }

  private def getFilesWithExtension(
      directory: java.nio.file.Path,
      extension: String): Seq[String] = {
    Files
      .walk(directory, FileVisitOption.FOLLOW_LINKS)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(extension))
      .map(p => p.toString)
      .toSeq
  }

  def resolveUdfConf(conf: java.util.Map[String, String]): Unit = {
    val sparkConf = SparkEnv.get.conf
    val udfLibPaths = if (isDriver) {
      sparkConf
        .getOption(BackendSettings.GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS)
        .orElse(sparkConf.getOption(BackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS))
    } else {
      sparkConf.getOption(BackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS)
    }

    udfLibPaths match {
      case Some(paths) =>
        conf.put(BackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS, getAllLibraries(paths, sparkConf))
      case None =>
    }
  }

  // Try to unpack archive. Throws exception if failed.
  private def unpack(source: File, destDir: File): File = {
    val sourceName = source.getName
    val dest = new File(destDir, sourceName)
    logInfo(
      s"Unpacking an archive $sourceName from ${source.getAbsolutePath} to ${dest.getAbsolutePath}")
    try {
      Utils.deleteRecursively(dest)
      Utils.unpack(source, dest)
    } catch {
      case e: Exception =>
        throw new GlutenException(
          s"Unpack ${source.toString} failed. Please check if it is an archive.",
          e)
    }
    dest
  }

  private def isRelativePath(path: String): Boolean = {
    try {
      val uri = new URI(path)
      !uri.isAbsolute && uri.getPath == path
    } catch {
      case _: Exception => false
    }
  }

  // Get the full paths of all libraries.
  // If it's a directory, get all files ends with ".so" recursively.
  private def getAllLibraries(files: String, sparkConf: SparkConf): String = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
    val master = sparkConf.getOption("spark.master")
    val isYarnCluster =
      master.isDefined && master.get.equals("yarn") && !Utils.isClientMode(sparkConf)
    val isYarnClient =
      master.isDefined && master.get.equals("yarn") && Utils.isClientMode(sparkConf)

    files
      .split(",")
      .map {
        f =>
          val file = new File(f)
          // Relative paths should be uploaded via --files or --archives
          if (isRelativePath(f)) {
            logInfo(s"resolve relative path: $f")
            if (isDriver && isYarnClient) {
              throw new IllegalArgumentException(
                "On yarn-client mode, driver only accepts absolute paths, but got " + f)
            }
            if (isYarnCluster || isYarnClient) {
              file
            } else {
              new File(SparkFiles.get(f))
            }
          } else {
            logInfo(s"resolve absolute URI path: $f")
            // Download or copy absolute paths to JniWorkspace.
            val uri = Utils.resolveURI(f)
            val name = file.getName
            val jniWorkspace = new File(JniWorkspace.getDefault.getWorkDir)
            if (!file.isDirectory && !f.endsWith(LIB_EXTENSION)) {
              val source = Utils
                .doFetchFile(uri.toString, Utils.createTempDir(), name, sparkConf, hadoopConf)
              unpack(source, jniWorkspace)
            } else {
              Utils.doFetchFile(uri.toString, jniWorkspace, name, sparkConf, hadoopConf)
            }
          }
      }
      .flatMap {
        f =>
          if (f.isDirectory) {
            getFilesWithExtension(f.toPath, LIB_EXTENSION)
          } else {
            Seq(f.toString)
          }
      }
      .mkString(",")
  }

  def getFunctionSignatures: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    val sparkContext = SparkContext.getActive.get
    val sparkConf = sparkContext.conf
    val udfLibPaths = sparkConf.getOption(BackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS)

    udfLibPaths match {
      case None =>
        Seq.empty
      case Some(_) =>
        new UdfJniWrapper().getFunctionSignatures()

        UDFNames.map {
          name =>
            (
              new FunctionIdentifier(name),
              new ExpressionInfo(classOf[UDFExpression].getName, name),
              (e: Seq[Expression]) => getUdfExpression(name)(e))
        }.toSeq ++ UDAFNames.map {
          name =>
            (
              new FunctionIdentifier(name),
              new ExpressionInfo(classOf[UserDefinedAggregateFunction].getName, name),
              (e: Seq[Expression]) => getUdafExpression(name)(e))
        }.toSeq
    }
  }

  private def getUdfExpression(name: String)(children: Seq[Expression]) = {
    val expressionType =
      UDFMap.getOrElse((name, children.map(_.dataType)), throw new IllegalStateException())
    UDFExpression(name, expressionType.dataType, expressionType.nullable, children)
  }

  private def getUdafExpression(name: String)(children: Seq[Expression]) = {
    val (expressionType, aggBufferAttributes) =
      UDAFMap.getOrElse((name, children.map(_.dataType)), throw new IllegalStateException())

    UserDefinedAggregateFunction(
      name,
      expressionType.dataType,
      expressionType.nullable,
      children,
      aggBufferAttributes)
  }
}
