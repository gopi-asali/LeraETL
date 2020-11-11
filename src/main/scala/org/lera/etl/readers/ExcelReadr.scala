package org.lera.etl.readers

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}


object ExcelReadr extends PipelineBase {

  private val logger: Logger = Logger.getLogger(ExcelReadr.getClass)

  /*
   *
   * Read data from source
   * @Param properties file
   * @return
   */

  def main(args: Array[String]): Unit = {

    val excelDF = readExcelData(properties)
    excelDF.show()
    excelDF.printSchema()
    println("Records in dataframe : " + excelDF.count())

    var dfPerSheet = Seq.empty[DataFrame]

//    Function call to read excel by sheets

    val numOfSheets = properties.getProperty("excel-sheets").toInt
    for (i <- 1 until numOfSheets - 1) {
      dfPerSheet = dfPerSheet :+ readExcelBySheets(
        spark,
        properties,
        i,
        excelDF
      )
    }

    for (dfs <- dfPerSheet) {
      println("dataframe: ")
      dfs.show()
    }

    val finalDF = dfPerSheet.reduce(_ unionAll _)
    finalDF.show()
    println("Records in final df : " + finalDF.count())

    spark.stop()
  }

  def readExcelData(properties: Properties): DataFrame = {

    val filePath = properties.getProperty("filepath")
    logger.info(s"Reading data from Excel file $filePath")

    val excelLibrary = properties.getProperty("lib")
    val dataAddress = properties.getProperty("address")
    val header = properties.getProperty("header") //Required field
    val treatEmptyValueAsNulls = properties.getProperty(
      "treatEmptyValuesAsNulls"
    ) // Optional, default: true
    val inferSchema = properties.getProperty("inferSchema") // Optional, default: false
    val maxRowsInMemory = properties.getProperty("maxRowsInMemory") // Optional, default None. If set, uses a streaming reader which can help with big files
    val excerptSize = properties.getProperty("excerptSize") // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from

    val dataFrame: DataFrame =
      properties.getProperty("load_type_excel").toLowerCase match {

        case "incremental" | "incr" =>
          val incrementalCondition: String = getIncrementalLoadFilterCondition()
          logger.info(s"Incremental condition is $incrementalCondition")
          val sourceDataFrame: DataFrame = readExcelFile(
            spark,
            filePath,
            excelLibrary,
            dataAddress,
            header,
            treatEmptyValueAsNulls,
            inferSchema,
            maxRowsInMemory,
            excerptSize
          )
          if (incrementalCondition.nonEmpty)
            sourceDataFrame.where(incrementalCondition)
          else
            sourceDataFrame
        case "fullloadtype" =>
          readExcelFile(
            spark,
            filePath,
            excelLibrary,
            dataAddress,
            header,
            treatEmptyValueAsNulls,
            inferSchema,
            maxRowsInMemory,
            excerptSize
          )
      }

    dataFrame

  }

  def getIncrementalLoadFilterCondition(): String = {
    "Empty function.. to be continued later"
  }

  /* The function below can be used in case where there are multiple sheets (from 2nd sheet onwards) without schema and first sheet's schema can be imposed on the rest */

  def readExcelFile(spark: SparkSession,
                    filePath: String,
                    excelLibrary: String,
                    dataAddress: String,
                    header: String,
                    treatEmptyValueAsNulls: String,
                    inferSchema: String,
                    maxRowsInMemory: String,
                    excerptSize: String): DataFrame = {

    spark.read
      .format(excelLibrary)
      .option("dataAddress", dataAddress)
      .option("header", header) // Required
      .option("treatEmptyValuesAsNulls", treatEmptyValueAsNulls) // Optional, default: true
      .option("inferSchema", inferSchema) // Optional, default: false
      .option("maxRowsInMemory", maxRowsInMemory) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", excerptSize) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .load(filePath)

  }

  def readExcelBySheets(spark: SparkSession,
                        properties: Properties,
                        sheetNum: Int,
                        mainDF: DataFrame): DataFrame = {

    val filepath = properties.getProperty("filepath")
    val schemaDF = mainDF.schema
    spark.read
      .format(properties.getProperty("lib"))
      .option("dataAddress", s"$sheetNum!")
      .schema(schemaDF)
      .option("header", properties.getProperty("header-second-sheet"))
      .option(
        "treatEmptyValuesAsNulls",
        properties.getProperty("treatEmptyValuesAsNulls")
      )
      .option("maxRowsInMemory", properties.getProperty("maxRowsInMemory"))
      .option("excerptSize", properties.getProperty("excerptSize"))
      .load(filepath)
  }

}
