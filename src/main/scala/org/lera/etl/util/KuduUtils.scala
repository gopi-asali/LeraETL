package org.lera.etl.util

import org.lera.etl.util.Constants.StringExpr

import org.lera.etl.util.ImpalaConnector._

import org.lera.etl.util.utils._
import org.lera.{ContextCreator, TableConfig}

import scala.util.{Failure, Success}

object KuduUtils extends ContextCreator{

  import org.apache.log4j.Logger
  import org.apache.spark.sql.DataFrame
  import scala.util.Try

  lazy val defaultNoOfPartitions : Int = getConf
    .getOption(key = "spark.kudu_default_partitions")
    .getOrElse(200.toString)
    .toInt

  //val test: ParMap[String, DataFrame] = ParMap.empty[String, DataFrame]

  val dataFrameCache: scala.collection.mutable.Map[String, DataFrame] = scala.collection.mutable.Map.empty[String, DataFrame]

  private val logger : Logger = Logger.getLogger(this.getClass)

  /*
   * This method will perform JDBC Kudu reading with partition if partition details are provided in
   * partition_column table
   * else will read without partition
   *
   * @param tableConf tableConfig
   * @param condition SQL where condition
   * @return
   * */

  def readKuduTableWithTableConfig(
    tableConf : TableConfig,
    condition : String = StringExpr.empty
  ) : DataFrame = {

    val cacheKey =
      s"${tableConf.source_database}_${tableConf.source_table}_${tableConf
        .load_type}_$condition"
    dataFrameCache.getOrElse(
        cacheKey, {
        val isHiveIntermediateRequired: Boolean = isHiveIntermediateEnabled(tableConf.source_system)
        val tableName : String = s"${tableConf.source_database}.${tableConf.source_table}"
        val whereCond : String = if(condition.isEmpty) StringExpr.empty else s"WHERE $condition"
	      val selectQuery : String = s"SELECT * FROM $tableName $whereCond"
	      val sourceDataFrame: DataFrame =
	        if(isHiveIntermediateRequired)
	          readKuduUsingHive(selectQuery, tableConf.target_database, tableConf.source_table)
	        else {
	          val partitionData: Array[PartitionTableConfig] = getPartitionTableValues(tableConf)
	          val kuduSelectQuery: String = s"($selectQuery)temp_table"

	          if(partitionData.nonEmpty){
	            logger
	            .info("Reading table with Partition details")
	  //        readKuduWithPartition(kuduSelectQuery,partitionData.head)(condition)
	            Predicates.readKuduWithPredicates(tableName, tableName, partitionData.head)(condition)
	          } else {
	            readKuduTable(kuduSelectQuery)(defaultNoOfPartitions)
	          }
	        }
        dataFrameCache.put(cacheKey,sourceDataFrame)
        sourceDataFrame
  }
        )
  }

  /*
   * Read partition table entry
   *
   * @param tableConf tableConfig
   * @return
   * */

  def getPartitionTableValues(
  tableConf : TableConfig
  ) : Array[PartitionTableConfig] = {

    import Constants._
    import org.apache.spark.sql.Encoders

    val partitionTable : String = s"$configDatabase.${getProperty(partitionCfgTableName)}"

    val selectColumns : String = Array(
    sourceSystem,
    sourceDataRegionName,
    database_name,
    tableName,
    partitionColumn,
    partitionColumnType,
    numOfPartitions,
    lowerBound,
    upperBound
    ).mkString(StringExpr.comma)

    val whereQuery : String =
      Array(
      s"$sourceSystem = '${tableConf.source_system}'",
      s"$sourceDataRegionName = '${tableConf.sourcedata_regionname}'",
      s"$database_name = '${tableConf.source_database}'",
      s"$tableName = '${tableConf.source_table}'"
      ).mkString(" AND ")

    readKuduTableWithColumns(
    partitionTable,
    selectColumns,
    whereQuery
    ).as[PartitionTableConfig](Encoders.product[PartitionTableConfig])
    .collect()
  }

  /*
   * Read Kudu tables with column names and conditions
   *
   * @param selectColumns select column names
   * @param where					condition
   * @param tableName			table name
   * @return
   * */

  def readKuduWithPartition(
    query : String,
    partitionConfig : PartitionTableConfig
  ): DataFrame = {

    val partitionDetails : Map[String,String] = Map(
    "partitionColumn" -> partitionConfig.partition_column,
    "numPartitions"   -> partitionConfig.number_of_partitions.toString,
    "upperBound"      -> partitionConfig.upper_bound.toString,
    "lowerBound"      -> partitionConfig.lower_bound.toString
    )

    readKuduTable(query,partitionDetails)

  }

  /*Read Kudu table data with partition information
   *
   * @param query select query
   * @param partitionConfig partition config
   *
   * @return
   *  */

  def readKuduWithCondition(tableName : String, where : Any) : DataFrame =
    readKuduTableWithColumns(tableName, StringExpr.empty, where.toString)

  /*
   * Read Kudu table using conditions
   *
   * @param tableName tableName
   * @param where condition
   * @return
   * */

  def readKuduTableWithColumns(tableName : String,
                               selectColumns : String = "",
                               where : String = ""): DataFrame = {
    val columns   = if(selectColumns.isEmpty) "*" else selectColumns
    val whereCond = if(where.isEmpty) "" else s"WHERE $where"
    val query =
      s"(SELECT $columns FROM $tableName $whereCond)${tableName.split("\\.")(1)}"
    readKuduTable(query)
  }

  def readKuduTableWithQuery(sourceSystem : String,
                             query : String,
                             targetDatabase : String,
                             tableName : String,
                             where : String = StringExpr.empty) : DataFrame = {
    dataFrameCache.getOrElse(
    query, {
      val isHiveEnabled : Boolean = isHiveIntermediateEnabled(sourceSystem)

      val whereCond : String = if(where.isEmpty) StringExpr.empty else s"WHERE sourceTable.$where"

	    val selectQuery : String = s"SELECT * FROM ($query)sourceTable $whereCond"
	    val sourceDataFrame =
	      if(isHiveEnabled) readKuduUsingHive(selectQuery, targetDatabase, tableName)
	      else {
	        val queryString = s"($selectQuery)$tableName"
	        readKuduTable(queryString)(defaultNoOfPartitions)
	      }
      dataFrameCache.put(query,sourceDataFrame)
      sourceDataFrame
    }
    )

  }

  /*
   * Check Hive based reading enable for the source system
   *
   * @param sourceSystem source system
   * @return
   * */

  def isHiveIntermediateEnabled(sourceSystem : String) : Boolean = {

    val sourceSystems = getConf
      .getOption(key = "spark.read_kudu_using_hive_source_systems")
      .getOrElse(StringExpr.empty)

    sourceSystems.split(StringExpr.comma).contains(sourceSystem)
  }

  /*
   * Read Kudu table using intermediate Hive Table
   *
   * @param query query for source table
   * @param targetDatabase target database where intermediate hive table is created
   * @param tableName table name for the intermediate table
   * @return
   * */

  def readKuduUsingHive(query : String, targetDatabase : String, tableName : String) :
    DataFrame = {

    val intermediateTable : String = s"$targetDatabase.${tableName}_intermediate"

    val dropStatement : String =
      s"DROP TABLE IF EXISTS $intermediateTable"

    executeQuery(dropStatement)
    val tableCreateStatement : String =
      s"CREATE TABLE $intermediateTable.STORED AS PARQUET AS SELECT * FROM ($query)tmp WHERE false"
	  executeQuery(tableCreateStatement)

    val columns : String = readHiveTable(intermediateTable).columns
      .map(col => s"'$col'")
      .mkString(StringExpr.comma)

    val insertQuery : String =
      s"INSERT INTO TABLE $intermediateTable ($columns) SELECT $columns FROM ($query)tmp"

    executeQuery(insertQuery)
    spark.catalog.refreshTable(intermediateTable)

    readHiveTable(tableName = s"$intermediateTable")
  }

  /*
   * Read Kudu table using jdbc connection and convert them into data set using spark session
   *
   * @param tableName Kudu table name or query
   * @return
   * */

  def readKuduTable(
    tableName : String,
    partitions : Map[String, String] = Map.empty
    )(implicit numOfPartition : Int = 1) : DataFrame = {

      Try{
        logger.info(s"Reading Kudu data for the table or query >> $tableName")
        val dataFrame : DataFrame = spark.read
        .format(source="jdbc")
        .option("charset","UTF8")
        .options(
        Map(
            "url"       -> connectionURL,
            "driver"    -> JDBCDriver,
            "dbtable"   -> tableName,
            "fetchsize" -> 100000.toString
        ) ++ partitions
        )
        .load()

        val partitionedDf =
          if(1 != numOfPartition) dataFrame.repartition(numOfPartition)
          else dataFrame

        partitionedDf.trimColumnTrailingSpace
      } match {
        case Success(outValue) => outValue
        case Failure(exception) =>
          logger.error(
          s"Read data from Kudu table failed due to ${exception.getMessage}"
          )

          throw exception
      }
    }
}
