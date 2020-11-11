package org.lera.etl.readers

import java.util.Properties
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends PipelineBase {

  private val logger: Logger = Logger.getLogger(ExcelReadr.getClass)

  /*
   *
   * Read data from source
   * @Param properties file
   * @return
   */

  def main(args: Array[String]): Unit = {

    val csvDF = readCSVData(properties)

    csvDF.show()
    csvDF.printSchema()
    println("Records in dataframe : " + csvDF.count())
  }

  def readCSVData(properties: Properties): DataFrame = {

    val filePath = properties.getProperty("filepath-CSV")
    logger.info(s"Reading data from CSV file $filePath")

    val header = properties.getProperty("csv-header")
    val delimiter = properties.getProperty("csv-delimiter")
    val inferSchema = properties.getProperty("csv-inferSchema")
    val mode = properties.getProperty("csv-mode") /* By default mode is PERMISSIVE (tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.*/
    val nullValue = properties.getProperty("csv-nullValue") //specifies a string that indicates a null value, any fields matching this string will be set as nulls in the DataFrame
    val timestampFormat = properties.getProperty("csv-dateFormat")
    val ignoreLeadingWhiteSpace =
      properties.getProperty("csv-ignoreLeadingWhiteSpace")
    val ignoreTrailingWhiteSpace =
      properties.getProperty("csv-ignoreTrailingWhiteSpace")
    val quoteChar = properties.getProperty("csv-quote")
    val escapeChar = properties.getProperty("csv-escape") //Escape characters inside the string identified by quotes option
    val commentChar = properties.getProperty("csv-comment")

    val dataFrame: DataFrame =
      properties.getProperty("load_type").toLowerCase match {

        case "incremental" | "incr" =>
          val incrementalCondition: String = getIncrementalLoadFilterCondition()
          logger.info(s"Incremental condition is $incrementalCondition")
          val sourceDataFrame: DataFrame = readCSVFile(
            spark,
            filePath,
            header,
            delimiter,
            inferSchema,
            mode,
            nullValue,
            timestampFormat,
            ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace,
            quoteChar,
            escapeChar,
            commentChar
          )
          if (incrementalCondition.nonEmpty)
            sourceDataFrame.where(incrementalCondition)
          else
            sourceDataFrame
        case "fullloadtype" =>
          readCSVFile(
            spark,
            filePath,
            header,
            delimiter,
            inferSchema,
            mode,
            nullValue,
            timestampFormat,
            ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace,
            quoteChar,
            escapeChar,
            commentChar
          )
      }
    dataFrame
  }

  def getIncrementalLoadFilterCondition(): String = {
    "Empty function.. to be continued later"
  }

  def readCSVFile(spark: SparkSession,
                  filePath: String,
                  header: String,
                  delimiter: String,
                  inferSchema: String,
                  mode: String,
                  nullValue: String,
                  timestampFormat: String,
                  ignoreLeadingWhiteSpace: String,
                  ignoreTrailingWhiteSpace: String,
                  quoteChar: String,
                  escapeChar: String,
                  commentChar: String): DataFrame = {

    spark.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", inferSchema)
      .option("mode", mode) // By default PERMISSIVE (tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.)
      .option("nullValue", nullValue) //specifies a string that indicates a null value, any fields matching this string will be set as nulls in the DataFrame
      .option("timestampFormat", timestampFormat)
      .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
      .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace)
      .option("quote", quoteChar)
      .option("escape", escapeChar) //Escape characters inside the string identified by quotes option
      .option("comment", commentChar)
      .csv(filePath)

  }
}
