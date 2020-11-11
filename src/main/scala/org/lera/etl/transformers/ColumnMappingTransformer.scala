package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.lera.TableConfig
import org.lera.etl.util.Constants.StringExpr

import scala.collection.mutable
import org.lera.etl.util.utils._
import org.lera.etl.util.Constants._
import scala.collection.parallel.ParSeq
import org.apache.spark.sql.functions._

object ColumnMappingTransformer extends BaseTransformer {

  val targetMappingColumns: mutable.Map[String, Array[String]] =
    mutable.Map.empty
  private val logger: Logger =
    Logger.getLogger(ColumnMappingTransformer.getClass)

  /*
   * Convert or map the target columns for source columns
   * @param dataFrameSeq tuples of table config and dataframe
   * @return
   *
   * */

  override def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)] = {

    val mappingConfigDataFrame: DataFrame =
      configHandler(dataFrameSeq.keys) {
        config => getDefaultTableConfigFunc(columnMappingConfigTableName)(config)
      }

    dataFrameSeq.flatMap(inDataSets => {

      val (tableConf: TableConfig, df: DataFrame) = inDataSets
      errorHandler(tableConf) {
        logger.info(
          s"Mapping target columns for table :: ${tableConf.target_table}"
        )

        val mappingColumnsList: Array[(String, String)] =
          getColumnMappingConfig(tableConf, mappingConfigDataFrame)

        val sourceColumns: Array[String] = df.columns.map(_.toLowerCase)
        val mappingSourceColumns: Array[String] = mappingColumnsList.map(_._1)

        val columnsNotAvailableInSource: Seq[String] =
          mappingSourceColumns.filterNot(name => {
            sourceColumns.contains(name)
          })

        val missingTargetColumns: Array[String] = mappingColumnsList
          .filter(
            columnTup => columnsNotAvailableInSource.contains(columnTup._1)
          )
          .map(_._2)

        validateMissingSourceColumn(
          tableConf.source_system,
          columnsNotAvailableInSource
        )

        val validColumnNames: Array[(String, String)] =
          mappingColumnsList.filterNot(
            valueTup => columnsNotAvailableInSource.contains(valueTup._1)
          )

        val renamedDf: DataFrame = renameDFColumnNames(df, validColumnNames)

        populateMissingTargetColumns(
          tableConf.target_table,
          mappingColumnsList,
          missingTargetColumns,
          renamedDf
        )
      }

    })
  }

  private def populateMissingTargetColumns(
    tableName: String,
    mappingColumnArray: Array[(String, String)],
    targetColumnForMissingSource: Array[String],
    renamedDf: DataFrame
  ): DataFrame = {
    val targetColumnNames: Array[String] =
      mappingColumnArray.map(_._2.toLowerCase)
    logger.info(s"Table $tableName mapping columns are $targetColumnNames")

    targetMappingColumns.put(tableName, targetColumnNames)

    val updatedDf: DataFrame = targetColumnForMissingSource
      .foldLeft(renamedDf)(
        (df, columnName) => df.withColumn(columnName, lit(literal = null))
      )

    logger.info(s"Target column mapping completed for table >> $tableName")
    updatedDf
  }

  private def getColumnMappingConfig(
    tableConf: TableConfig,
    configDataFrame: DataFrame
  ): Array[(String, String)] = {

    val whereQuery: String = Map(
      sourceTable -> tableConf.source_table,
      targetTable -> tableConf.target_table
    ).toWhereCondition.ignoreCaseInSQL

    val sourceColumnMapping: DataFrame = configDataFrame
      .where(whereQuery)
      .select(sourceColumn, targetColumn)

    sourceColumnMapping.rdd
      .map(row => {
        row
          .getAs[String](sourceColumn)
          .trim
          .toLowerCase -> row.getAs[String](targetColumn).trim.toLowerCase
      })
      .collect()
  }

  private def validateMissingSourceColumn(
    sourceSystem: String,
    missingMappingColumns: Seq[String]
  ): Unit = {
    if (missingMappingColumns.nonEmpty && !isSourceEnabled(
          skipMissingSourceColumn,
          sourceSystem
        )) {
      throw new Exception(
        s"Missing mapping columns are : ${missingMappingColumns.mkString(StringExpr.comma)}"
      )
    } else if (missingMappingColumns.nonEmpty) {
      logger.warn(
        s"Missing mapping columns are : ${missingMappingColumns.mkString(StringExpr.comma)}"
      )
    }
  }
}
