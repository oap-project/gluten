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
package org.apache.gluten.utils

import org.apache.gluten.expression.ExpressionMappings

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

import java.io.{File, PrintWriter}
import java.util

/**
 * This class can export all spark expressions and statistic supported of ch backend. Export string
 * can be convert to markdown.
 */
object ExpressionDocUtil {
  val markdownFormat: String = "| %s | %s | %s | %s | %s |\n"
  val alreadySupported: Map[String, String] = {
    ExpressionMappings.expressionsMap.map(x => (x._1.getName, x._2))
  }

  val groupedExpressions: util.Map[String, util.List[ExpressionInfo]] = {
    val groups = new util.HashMap[String, util.List[ExpressionInfo]]()
    FunctionRegistry.expressions.foreach(
      expression => {
        val (expr, _) = expression._2
        groups.computeIfAbsent(expr.getGroup, _ => new util.ArrayList[ExpressionInfo]()).add(expr)
      })
    groups
  }

  def main(args: Array[String]): Unit = {

    val groupOrdered = new util.ArrayList[String](groupedExpressions.keySet())
    util.Collections.sort(groupOrdered)

    val content = new StringBuffer()
    val summary = new StringBuffer()
    content.append(
      markdownFormat.format("Group", "Expression", "Spark Class Name", "CH Backend", "Examples"))
    content.append(markdownFormat.format("----", "----", "----", "----", "----"))

    var total = 0
    var support = 0
    groupOrdered.forEach(
      name => {
        var g_total_cnt = 0
        var g_support_cnt = 0
        content.append(markdownFormat.format(name, "", "", "", ""))
        val expression_sort = new util.ArrayList[ExpressionInfo](groupedExpressions.get(name))
        expression_sort.sort((o1, o2) => o1.getName.compareTo(o2.getName))
        expression_sort.forEach(
          expr => {
            val support_txt = isSupport(expr)
            total = total + 1
            g_total_cnt = g_total_cnt + 1
            if ("True".equalsIgnoreCase(support_txt)) {
              support = support + 1
              g_support_cnt = g_support_cnt + 1
            }
            content.append(
              markdownFormat
                .format(
                  "",
                  replaceEnter(expr.getName),
                  replaceEnter(expr.getClassName),
                  support_txt,
                  replaceEnter(expr.getExamples)))
          })
        summary.append(
          "`%s` total expressions: %d, Supported expression:%d, Supported Rate: %.2f%%\n\n"
            .format(name, g_total_cnt, g_support_cnt, g_support_cnt * 100.0 / g_total_cnt))
      })
    val writer = new PrintWriter(
      new File("/home/admin123/Documents/work/code/Gluten-Dev/expressions.md"))

    writer.write(
      "Total expressions: %d, Supported expression:%d, Supported Rate: %.2f%%\n\n"
        .format(total, support, support * 100.0 / total))
    writer.write(summary.toString)
    writer.write("\n\n")
    writer.write(content.toString)
    writer.close()
  }

  def supportExpression(): util.Set[String] = {
    val result = new util.HashSet[String]()
    groupedExpressions.forEach(
      (_, expressionInfos) => {
        expressionInfos
          .forEach(
            expressionInfo => {
              if ("True".equalsIgnoreCase(isSupport(expressionInfo))) {
                result.add(expressionInfo.getClassName)
              }
            })
      })

    result
  }

  def isSupport(expr: ExpressionInfo): String = {
    val substraitName = alreadySupported.getOrElse(expr.getClassName, "")

    if (substraitName.isEmpty) {
      return "False"
    }

    if (expr.getClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")) {
      if (CHExpressionUtil.CH_AGGREGATE_FUNC_BLACKLIST.contains(substraitName)) {
        return "False"
      }
    } else {
      CHExpressionUtil.CH_BLACKLIST_SCALAR_FUNCTION.get(substraitName) match {
        case Some(_) => return "False"
        case _ =>
      }
    }

    "True"
  }

  def replaceEnter(str: String): String = {
    str.replace("\n", "<br />")
  }

}
