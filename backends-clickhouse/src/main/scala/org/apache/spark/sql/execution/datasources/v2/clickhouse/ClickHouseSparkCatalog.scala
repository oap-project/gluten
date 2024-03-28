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
package org.apache.spark.sql.execution.datasources.v2.clickhouse
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability.V1_BATCH_WRITE
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.DeltaTableIdentifier.gluePermissionError
import org.apache.spark.sql.delta.catalog.{ClickHouseTableV2, TempClickHouseTableV2}
import org.apache.spark.sql.delta.commands.{CreateDeltaTableCommand, TableCreationModes, WriteIntoDelta}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.utils.CHDataSourceUtils
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

class ClickHouseSparkCatalog
  extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SupportsPathIdentifier
  with DeltaLogging {

  val spark = SparkSession.active

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (CHDataSourceUtils.isClickHouseDataSourceName(getProvider(properties))) {
      createClickHouseTable(
        ident,
        schema,
        partitions,
        properties,
        Map.empty,
        sourceQuery = None,
        TableCreationModes.Create)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }
  }

  /**
   * Creates a ClickHouse table
   *
   * @param ident
   *   The identifier of the table
   * @param schema
   *   The schema of the table
   * @param partitions
   *   The partition transforms for the table
   * @param allTableProperties
   *   The table properties that configure the behavior of the table or provide information about
   *   the table
   * @param writeOptions
   *   Options specific to the write during table creation or replacement
   * @param sourceQuery
   *   A query if this CREATE request came from a CTAS or RTAS
   * @param operation
   *   The specific table creation mode, whether this is a Create/Replace/Create or Replace
   */
  private def createClickHouseTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String],
      writeOptions: Map[String, String],
      sourceQuery: Option[DataFrame],
      operation: TableCreationModes.CreationMode): Table = {
    val (partitionColumns, maybeBucketSpec) =
      SparkShimLoader.getSparkShims.convertPartitionTransforms(partitions)
    var newSchema = schema
    var newPartitionColumns = partitionColumns
    var newBucketSpec = maybeBucketSpec

    // Delta does not support bucket feature, so save the bucket infos into properties if exists.
    val tableProperties =
      ClickHouseConfig.createMergeTreeConfigurations(allTableProperties, newBucketSpec)

    val isByPath = isPathIdentifier(ident)
    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    val locUriOpt = location.map(CatalogUtils.stringToURI)
    val storage = DataSource
      .buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = locUriOpt)
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val id = TableIdentifier(ident.name(), ident.namespace().lastOption)
    val loc = new Path(locUriOpt.getOrElse(spark.sessionState.catalog.defaultTablePath(id)))
    val commentOpt = Option(allTableProperties.get("comment"))

    val tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(ClickHouseConfig.ALT_NAME),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tableProperties,
      comment = commentOpt
    )

    val withDb = verifyTableAndSolidify(tableDesc, None)

    val writer = sourceQuery.map {
      df =>
        WriteIntoDelta(
          DeltaLog.forTable(spark, loc),
          operation.mode,
          new DeltaOptions(withDb.storage.properties, spark.sessionState.conf),
          withDb.partitionColumnNames,
          withDb.properties ++ commentOpt.map("comment" -> _),
          df,
          schemaInCatalog = if (newSchema != schema) Some(newSchema) else None
        )
    }
    try {
      ClickHouseTableV2.temporalThreadLocalCHTable.set(
        new TempClickHouseTableV2(spark, Some(withDb)))

      CreateDeltaTableCommand(
        withDb,
        getExistingTableIfExists(tableDesc),
        operation.mode,
        writer,
        operation = operation,
        tableByPath = isByPath).run(spark)
    } finally {
      ClickHouseTableV2.temporalThreadLocalCHTable.remove()
    }

    logInfo(s"create table ${ident.toString} successfully.")
    val loadedNewTable = loadTable(ident)
    loadedNewTable
  }

  /** Performs checks on the parameters provided for table creation for a ClickHouse table. */
  private def verifyTableAndSolidify(
      tableDesc: CatalogTable,
      query: Option[LogicalPlan]): CatalogTable = {

    val schema = query
      .map {
        plan =>
          assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
          plan.schema.asNullable
      }
      .getOrElse(tableDesc.schema)

    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false
    ) // Delta is case insensitive

    val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = tableDesc.properties)
  }

  /** Checks if a table already exists for the provided identifier. */
  private def getExistingTableIfExists(table: CatalogTable): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathIdentifier(table)) return None
    val tableExists = catalog.tableExists(table.identifier)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table.identifier)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(
          s"${table.identifier} is a view. You may not write data into a view.")
      }
      if (!CHDataSourceUtils.isClickHouseTable(oldTable.provider)) {
        throw new AnalysisException(
          s"${table.identifier} is not a ClickHouse table. Please drop " +
            s"this table first if you would like to recreate it.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider")).getOrElse(ClickHouseConfig.NAME)
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if CHDataSourceUtils.isDeltaTable(v1.catalogTable) =>
          new ClickHouseTableV2(
            spark,
            new Path(v1.catalogTable.location),
            catalogTable = Some(v1.catalogTable),
            tableIdentifier = Some(ident.toString))
        case o =>
          o
      }
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
          if isPathIdentifier(ident) =>
        newDeltaPathTable(ident)
      case e: AnalysisException if gluePermissionError(e) && isPathIdentifier(ident) =>
        logWarning(
          "Received an access denied error from Glue. Assuming this " +
            s"identifier ($ident) is path based.",
          e)
        newDeltaPathTable(ident)
    }
  }

  private def newDeltaPathTable(ident: Identifier): ClickHouseTableV2 = {
    new ClickHouseTableV2(spark, new Path(ident.name()))
  }

  /** support to delete mergetree data from the external table */
  override def purgeTable(ident: Identifier): Boolean = {
    try {
      loadTable(ident) match {
        case t: ClickHouseTableV2 =>
          val tableType = t.properties().getOrDefault("Type", "")
          // file-based or external table
          val isExternal = tableType.isEmpty || tableType.equalsIgnoreCase("external")
          val tablePath = t.rootPath
          // first delete the table metadata
          val deletedTable = super.dropTable(ident)
          if (deletedTable && isExternal) {
            val fs = tablePath.getFileSystem(spark.sessionState.newHadoopConf())
            // delete all data if there is a external table
            fs.delete(tablePath, true)
          }
          true
        case _ => super.purgeTable(ident)
      }
    } catch {
      case _: Exception =>
        false
    }
  }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageReplace") {
      if (CHDataSourceUtils.isClickHouseDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(ident, schema, partitions, properties, TableCreationModes.Replace)
      } else {
        super.dropTable(ident)
        BestEffortStagedTable(ident, super.createTable(ident, schema, partitions, properties), this)
      }
    }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreateOrReplace") {
      if (CHDataSourceUtils.isClickHouseDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(
          ident,
          schema,
          partitions,
          properties,
          TableCreationModes.CreateOrReplace)
      } else {
        try super.dropTable(ident)
        catch {
          case _: NoSuchDatabaseException => // this is fine
          case _: NoSuchTableException => // this is fine
        }
        BestEffortStagedTable(ident, super.createTable(ident, schema, partitions, properties), this)
      }
    }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreate") {
      if (CHDataSourceUtils.isClickHouseDataSourceName(getProvider(properties))) {
        new StagedDeltaTableV2(ident, schema, partitions, properties, TableCreationModes.Create)
      } else {
        BestEffortStagedTable(ident, super.createTable(ident, schema, partitions, properties), this)
      }
    }

  private class StagedDeltaTableV2(
      ident: Identifier,
      override val schema: StructType,
      val partitions: Array[Transform],
      override val properties: util.Map[String, String],
      operation: TableCreationModes.CreationMode)
    extends StagedTable
    with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = Map.empty

    override def commitStagedChanges(): Unit =
      recordFrameProfile("DeltaCatalog", "commitStagedChanges") {
        val conf = spark.sessionState.conf
        val props = new util.HashMap[String, String]()
        // Options passed in through the SQL API will show up both with an "option." prefix and
        // without in Spark 3.1, so we need to remove those from the properties
        val optionsThroughProperties = properties.asScala.collect {
          case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
        }.toSet
        val sqlWriteOptions = new util.HashMap[String, String]()
        properties.asScala.foreach {
          case (k, v) =>
            if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
              // Do not add to properties
              props.put(k, v)
            } else if (optionsThroughProperties.contains(k)) {
              sqlWriteOptions.put(k, v)
            }
        }
        if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
          writeOptions = sqlWriteOptions.asScala.toMap
        }
        if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
          // Legacy behavior
          writeOptions.foreach { case (k, v) => props.put(k, v) }
        } else {
          writeOptions.foreach {
            case (k, v) =>
              // Continue putting in Delta prefixed options to avoid breaking workloads
              if (k.toLowerCase(Locale.ROOT).startsWith("delta.")) {
                props.put(k, v)
              }
          }
        }
        createClickHouseTable(
          ident,
          schema,
          partitions,
          props,
          writeOptions,
          asSelectQuery,
          operation)
      }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      writeOptions = info.options.asCaseSensitiveMap().asScala.toMap
      new DeltaV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a Delta table.
     */
    private class DeltaV1WriteBuilder extends WriteBuilder {
      override def build(): V1Write = new V1Write {
        override def toInsertableRelation(): InsertableRelation = {
          new InsertableRelation {
            override def insert(data: DataFrame, overwrite: Boolean): Unit = {
              asSelectQuery = Option(data)
            }
          }
        }
      }
    }
  }

  private case class BestEffortStagedTable(ident: Identifier, table: Table, catalog: TableCatalog)
    extends StagedTable
    with SupportsWrite {
    override def abortStagedChanges(): Unit = catalog.dropTable(ident)

    override def commitStagedChanges(): Unit = {}

    // Pass through
    override def name(): String = table.name()
    override def schema(): StructType = table.schema()
    override def partitioning(): Array[Transform] = table.partitioning()
    override def capabilities(): util.Set[TableCapability] = table.capabilities()
    override def properties(): util.Map[String, String] = table.properties()

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = table match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ => throw DeltaErrors.unsupportedWriteStagedTable(name)
    }
  }
}

/**
 * A trait for handling table access through clickhouse.`/some/path`. This is a stop-gap solution
 * until PathIdentifiers are implemented in Apache Spark.
 */
trait SupportsPathIdentifier extends TableCatalog {
  self: ClickHouseSparkCatalog =>

  protected lazy val catalog: SessionCatalog = spark.sessionState.catalog

  override def tableExists(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)) {
      val path = new Path(ident.name())
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(path) && fs.listStatus(path).nonEmpty
    } else {
      super.tableExists(ident)
    }
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasClickHouseNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  private def hasClickHouseNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 &&
    CHDataSourceUtils.isClickHouseDataSourceName(ident.namespace().head)
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(Identifier.of(table.identifier.database.toArray, table.identifier.table))
  }
}
