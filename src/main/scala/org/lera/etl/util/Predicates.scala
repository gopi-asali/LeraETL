package org.lera.etl.util
import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.lera.ContextCreator
import org.lera.etl.util.Constants._
import org.lera.etl.util.ImpalaConnector.{JDBCDriver, connectionURL, statement}
import scala.util.Try

object Predicates extends ContextCreator {

  /*
  * Read kudu data with predicates to have multiple partitions
  *
  * @param tableName table name
  * @param query query
  * @param partitionConfig partition config
  * @return
  *
  **/

  def readKuduWithPredicates(
                              tableName : String,
                              query : String,
                              partitionConfig : PartitionTableConfig
                            ) (additionalCondition : String = StringExpr.empty) : DataFrame = {

    val conditions : String = getPredicates(tableName,partitionConfig)
    val conditionArray : Array[String] =
      conditions.substring(0,conditions.length - 4).split(",")

    val finalCondArray: Array[String] =
      if(additionalCondition.nonEmpty) conditionArray.map(cond => {
        s"$cond AND $additionalCondition"
      })
      else conditionArray

    spark.read.jdbc(connectionURL, query, finalCondArray, getJDBCProperties
    )
  }

  /*
  * Get predicate conditions
  *
  * @param tableName table name
  * @param partitionConfig partition info
  * @return
  **/

  def getPredicates(tableName : String, partitionConfig: PartitionTableConfig): String = {
    val numOfPartitions : Int = partitionConfig.number_of_partitions
    val partitionColumn = partitionConfig.partition_column.trim.toLowerCase
    val partType: String = Try(partitionConfig.partition_column_type.toLowerCase).getOrElse(null)
    val sourceType: String = getColumnTypes(tableName).getOrElse(
      partitionColumn,
      throw new ETLException(
        s"Column $partitionColumn not found in $tableName"
      )
    )

    val minAndMax : (String,String) =
      findMinMaxValue(tableName,partitionColumn,partType)
        .getOrElse(
          (
            partitionConfig.lower_bound.toString,
            partitionConfig.upper_bound.toString
          )
        )

    def conditionCreator(partitionColumnType: String): String = {
      partitionColumnType match {
        case x if null == x || x.trim.isEmpty => conditionCreator(sourceType)
        case "timestamp" =>
          import java.text.SimpleDateFormat
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
          val minDate = dateFormat.parse(minAndMax._1)
          val maxDate = dateFormat.parse(minAndMax._2)

          val onePartition
          : Int = (minDate.getTime - maxDate.getTime).toInt / numOfPartitions
          (
            for(i <- 1 to numOfPartitions)
              yield
                s"$partitionColumn <= CAST(${onePartition * i} AS TIMESTAMP), $partitionColumn > CAST(${onePartition * i} AS TIMESTAMP) AND "
            ).mkString("")
        case "string" =>
          val onePartition
          : Int = (minAndMax._2(0) - minAndMax._1(0)) / numOfPartitions
          (
            for (i <- 1 to numOfPartitions)
              yield
                s"$partitionColumn <= chr(${onePartition * i}), $partitionColumn > chr(${onePartition * i}) AND "
            ).mkString("")
        case _ =>
          val onePartition
          : Int = (minAndMax._2.toInt - minAndMax._1.toInt) / numOfPartitions
          (
            for (i <- 1 to numOfPartitions) yield{
              if(sourceType.toLowerCase == "string")
                s"$partitionColumn <= '${onePartition * i}', $partitionColumn > '${onePartition * i}' AND "
              else
                s"$partitionColumn <= ${onePartition * i}, $partitionColumn > ${onePartition * i} AND "
            }
            ).mkString("")
      }
    }

    conditionCreator(partType)

  }

  /*
  * Find max value for partitioning
  * @param table table name
  * @param column column
  * @return
  **/

  def findMinMaxValue(table : String,
                      column : String,
                      columnType : String): Option[(String,String)] = {

    val condition : String =
      if(null != columnType && columnType.trim.nonEmpty && columnType.toLowerCase != "string")
        s"CAST($column AS $columnType)"
      else column

    val resultSet =
      statement.executeQuery(
         s"SELECT max($column) AS max_value, min($column) AS min_value from $table WHERE $condition IS NOT NULL"
      )

    var valueOption: Option[(String, String)] = None
    while(resultSet.next()){
      valueOption =
        Some(resultSet.getString( "min_value"), resultSet.getString( "max_value"))
    }
    valueOption
  }

  /*
  * Get table not null columns
  *
  * @param table table name
  * @return
  **/

  def getColumnTypes(table : String): Map[String,String] = {
    import scala.collection.mutable
    val resultSet = statement.executeQuery( s"DESCRIBE $table")

    val columnNames = mutable.Map.empty[String,String]
    while(resultSet.next()) {
      val columnName : String = resultSet.getString( "name")
      val colType : String = resultSet.getString( "type")
      columnNames.put(columnName, colType)
    }

    resultSet.close()
    columnNames.toMap
  }

  /*
  * Jdbc properties
  *
  * @return
  **/

  def getJDBCProperties : Properties = {
    import java.util.Properties
    val properties = new Properties()
    properties.setProperty("driver",JDBCDriver)
    properties.setProperty("fetchsize","100000")
    properties.setProperty("pushDownPredicate","false")
    properties
  }

}
