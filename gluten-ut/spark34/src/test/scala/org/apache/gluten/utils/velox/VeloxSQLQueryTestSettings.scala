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
package org.apache.gluten.utils.velox

import org.apache.gluten.utils.SQLQueryTestSettings

object VeloxSQLQueryTestSettings extends SQLQueryTestSettings {
  override def getSupportedSQLQueryTests: Set[String] = SUPPORTED_SQL_QUERY_LIST

  override def getOverwriteSQLQueryTests: Set[String] = OVERWRITE_SQL_QUERY_LIST

  // Put relative path to "/path/to/spark/sql/core/src/test/resources/sql-tests/inputs" in this list
  val SUPPORTED_SQL_QUERY_LIST: Set[String] = Set(
    "bitwise.sql",
    "cast.sql",
    "change-column.sql",
    "charvarchar.sql",
    "columnresolution-negative.sql",
    "columnresolution-views.sql",
    "columnresolution.sql",
    "comments.sql",
    "comparator.sql",
    "count.sql",
    "cross-join.sql",
    "csv-functions.sql",
    "cte-legacy.sql",
    "cte-nested.sql",
    "cte-nonlegacy.sql",
    "cte.sql",
    "current_database_catalog.sql",
    "date.sql",
    "datetime-formatting-invalid.sql",
    // Velox had different handling for some illegal cases.
    // "datetime-formatting-legacy.sql",
    // "datetime-formatting.sql",
    "datetime-legacy.sql",
    "datetime-parsing-invalid.sql",
    "datetime-parsing-legacy.sql",
    "datetime-parsing.sql",
    "datetime-special.sql",
    "decimalArithmeticOperations.sql",
    // "describe-part-after-analyze.sql",
    "describe-query.sql",
    "describe-table-after-alter-table.sql",
    "describe-table-column.sql",
    "describe.sql",
    "except-all.sql",
    "except.sql",
    "extract.sql",
    "group-by-filter.sql",
    "group-by-ordinal.sql",
    "grouping_set.sql",
    "having.sql",
    "ignored.sql",
    "ilike-all.sql",
    "ilike-any.sql",
    "inline-table.sql",
    "inner-join.sql",
    "intersect-all.sql",
    "interval.sql",
    "join-empty-relation.sql",
    "join-lateral.sql",
    "json-functions.sql",
    "like-all.sql",
    "like-any.sql",
    "limit.sql",
    "literals.sql",
    "map.sql",
    "misc-functions.sql",
    "natural-join.sql",
    "null-handling.sql",
    "null-propagation.sql",
    "operators.sql",
    "order-by-nulls-ordering.sql",
    "order-by-ordinal.sql",
    "outer-join.sql",
    "parse-schema-string.sql",
    "pivot.sql",
    "pred-pushdown.sql",
    "predicate-functions.sql",
    "query_regex_column.sql",
    "random.sql",
    "regexp-functions.sql",
    "show-create-table.sql",
    "show-tables.sql",
    "show-tblproperties.sql",
    "show-views.sql",
    "show_columns.sql",
    "sql-compatibility-functions.sql",
    "string-functions.sql",
    "struct.sql",
    "subexp-elimination.sql",
    "table-aliases.sql",
    "table-valued-functions.sql",
    "tablesample-negative.sql",
    "subquery/exists-subquery/exists-aggregate.sql",
    "subquery/exists-subquery/exists-basic.sql",
    "subquery/exists-subquery/exists-cte.sql",
    "subquery/exists-subquery/exists-having.sql",
    "subquery/exists-subquery/exists-joins-and-set-ops.sql",
    "subquery/exists-subquery/exists-orderby-limit.sql",
    "subquery/exists-subquery/exists-within-and-or.sql",
    "subquery/in-subquery/in-basic.sql",
    "subquery/in-subquery/in-group-by.sql",
    "subquery/in-subquery/in-having.sql",
    "subquery/in-subquery/in-joins.sql",
    "subquery/in-subquery/in-limit.sql",
    "subquery/in-subquery/in-multiple-columns.sql",
    "subquery/in-subquery/in-order-by.sql",
    "subquery/in-subquery/in-set-operations.sql",
    "subquery/in-subquery/in-with-cte.sql",
    "subquery/in-subquery/nested-not-in.sql",
    "subquery/in-subquery/not-in-group-by.sql",
    "subquery/in-subquery/not-in-joins.sql",
    "subquery/in-subquery/not-in-unit-tests-multi-column.sql",
    "subquery/in-subquery/not-in-unit-tests-multi-column-literal.sql",
    "subquery/in-subquery/not-in-unit-tests-single-column.sql",
    "subquery/in-subquery/not-in-unit-tests-single-column-literal.sql",
    "subquery/in-subquery/simple-in.sql",
    "subquery/negative-cases/invalid-correlation.sql",
    "subquery/negative-cases/subq-input-typecheck.sql",
    "subquery/scalar-subquery/scalar-subquery-predicate.sql",
    "subquery/subquery-in-from.sql",
    "postgreSQL/aggregates_part1.sql",
    "postgreSQL/aggregates_part2.sql",
    "postgreSQL/aggregates_part3.sql",
    "postgreSQL/aggregates_part4.sql",
    "postgreSQL/boolean.sql",
    "postgreSQL/case.sql",
    "postgreSQL/comments.sql",
    "postgreSQL/create_view.sql",
    "postgreSQL/date.sql",
    "postgreSQL/float4.sql",
    "postgreSQL/insert.sql",
    "postgreSQL/int2.sql",
    "postgreSQL/int4.sql",
    "postgreSQL/int8.sql",
    "postgreSQL/interval.sql",
    "postgreSQL/join.sql",
    "postgreSQL/limit.sql",
    "postgreSQL/numeric.sql",
    "postgreSQL/select.sql",
    "postgreSQL/select_distinct.sql",
    "postgreSQL/select_having.sql",
    "postgreSQL/select_implicit.sql",
    "postgreSQL/strings.sql",
    "postgreSQL/text.sql",
    "postgreSQL/timestamp.sql",
    "postgreSQL/union.sql",
    "postgreSQL/window_part1.sql",
    "postgreSQL/window_part2.sql",
    "postgreSQL/window_part3.sql",
    "postgreSQL/window_part4.sql",
    "postgreSQL/with.sql",
    "datetime-special.sql",
    "timestamp-ansi.sql",
    "timestamp.sql",
    "arrayJoin.sql",
    "binaryComparison.sql",
    "booleanEquality.sql",
    "caseWhenCoercion.sql",
    "concat.sql",
    "dateTimeOperations.sql",
    "decimalPrecision.sql",
    "division.sql",
    "elt.sql",
    "ifCoercion.sql",
    "implicitTypeCasts.sql",
    "inConversion.sql",
    "mapZipWith.sql",
    "promoteStrings.sql",
    "stringCastAndExpressions.sql",
    "widenSetOperationTypes.sql",
    "windowFrameCoercion.sql",
    "timestamp-ltz.sql",
    "timestamp-ntz.sql",
    "timezone.sql",
    "transform.sql",
    "try_arithmetic.sql",
    "try_cast.sql",
    "udaf.sql",
    "union.sql",
    "using-join.sql",
    "window.sql",
    "udf-union.sql",
    "udf-window.sql",
    "ansi/cast.sql",
    "ansi/decimalArithmeticOperations.sql",
    "ansi/map.sql",
    "ansi/datetime-parsing-invalid.sql",
    "ansi/string-functions.sql",
    "ansi/interval.sql",
    "ansi/date.sql",
    "ansi/timestamp.sql",
    "ansi/try_arithmetic.sql",
    "ansi/literals.sql",
    "timestampNTZ/timestamp-ansi.sql",
    "timestampNTZ/timestamp.sql",
    "udf/udf-intersect-all.sql - Scala UDF",
    "udf/udf-except-all.sql - Scala UDF",
    "udf/udf-udaf.sql - Scala UDF",
    "udf/udf-except.sql - Scala UDF",
    "udf/udf-pivot.sql - Scala UDF",
    "udf/udf-inline-table.sql - Scala UDF",
    "udf/postgreSQL/udf-select_having.sql - Scala UDF",
    "typeCoercion/native/windowFrameCoercion.sql",
    "typeCoercion/native/decimalPrecision.sql",
    "typeCoercion/native/ifCoercion.sql",
    "typeCoercion/native/dateTimeOperations.sql",
    "typeCoercion/native/booleanEquality.sql",
    "typeCoercion/native/mapZipWith.sql",
    "typeCoercion/native/caseWhenCoercion.sql",
    "typeCoercion/native/widenSetOperationTypes.sql",
    "typeCoercion/native/promoteStrings.sql",
    "typeCoercion/native/stringCastAndExpressions.sql",
    "typeCoercion/native/inConversion.sql",
    "typeCoercion/native/division.sql",
    "typeCoercion/native/mapconcat.sql"
  )

  val OVERWRITE_SQL_QUERY_LIST: Set[String] = Set(
    // Velox corr has better computation logic but it fails Spark's precision check.
    // Remove -- SPARK-24369 multiple distinct aggregations having the same argument set
    "group-by.sql",
    "udf/udf-group-by.sql",
    // Exception string doesn't match for
    // SELECT (SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a) t) AS b
    "subquery/scalar-subquery/scalar-subquery-select.sql"
  )
}