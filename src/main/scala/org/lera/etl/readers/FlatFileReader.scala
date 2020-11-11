package org.lera.etl.readers

import java.io.FileNotFoundException

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.lera.TableConfig
import org.lera.etl.util.Constants.{StringExpr, _}
import org.lera.etl.util.Enums.Readers._
import org.lera.etl.util.Enums._
import org.lera.etl.util.KuduUtils._

object FlatFileReader extends Reader {

  import org.lera.etl.DataFrameImplicits

  private val logger: Logger = Logger.getLogger(FlatFileReader.getClass)

  override def readData(tableConf: TableConfig): (TableConfig, DataFrame) = {
    if (!isFilePathExists(tableConf.file_path) && tableConf.source_table_type != csv) {
      throw new FileNotFoundException(
        s"File does not exist in : ${tableConf.file_path}"
      )
    }

    val df: DataFrame =
      Readers.fromString(tableConf.source_table_type).get match {
        case EXCEL =>
          val targetSchema = readKuduWithCondition(
            tableName =
              s"${tableConf.target_database}.${tableConf.target_table}",
            where = false
          ).schema
          readExcel(tableConf.file_path, targetSchema)

        case JSON => spark.read.json(tableConf.file_path)
        case CSV  => readCSV(tableConf.file_path, tableConf.source_system)
        case _    => spark.read.textFile(tableConf.file_path).toDF
      }

    (tableConf, df)
  }

  /*
   * Read excel file from local or hdfs location
   *
   * @param fileName file location
   * @return
   * */

  def readExcel(fileName: String, schema: StructType): DataFrame = {

    spark.read
      .format(source = "com.crealytics.spark.excel")
      .schema(schema)
      .option("sheetName", "Sheet1")
      .option("header", true.toString)
      .load(fileName)
      .filter(
        row =>
          !(
            row
              .mkString(StringExpr.empty)
              .replace("null", StringExpr.empty)
              .isEmpty && row.length > 0
        )
      )
      .trimColumnTrailingSpace()
  }

  /*
   * Get CSV file to dataframe
   *
   * @param fileLocation file location
   * @param sourceSystem system
   * @return Number of files
   * */

  def readCSV(fileLocation: String, sourceSystem: String): DataFrame = {
    /* lazy val numOfFiles = getNumberOfFiles(fileLocation)
     * if(fileLocation.startsWith("file:///") || numOfFiles ==1)*/

    spark.read
      .format("csv")
      .option("header", true.toString)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .load(path = s"$fileLocation")

    /* else if(numOfFiles > 1) {
   * throw new Exception(
   * s"Due to number of source files more than one at location $fileLocation"
   * )
   * } else {
   * throw new FileNotFoundException(
   * s"Number of files is 0. Please provide the CSV file for source system $sourceSystem"
   * )
   * }*/
  }

}
