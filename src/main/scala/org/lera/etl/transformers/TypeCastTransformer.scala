package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}
import org.lera.TableConfig
import org.lera.etl.util.Constants._
import scala.collection.parallel.ParSeq

import org.lera.etl.util.utils._
import org.apache.spark.sql.functions._

object TypeCastTransformer extends BaseTransformer {

  private val logger: Logger = Logger.getLogger(TypeCastTransformer.getClass)

  override def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)] = {

    dataFrameSeq.flatMap(data => {
      errorHandler(data._1) {
        val (tableConf, dataDF) = data
        logger.info(s"Data type casting for table ${tableConf.target_table}")

        val sourceTableColumns: Seq[String] = dataDF.columns.map(_.toLowerCase)
        val targetSchema: StructType = targetTableSchema(tableConf)
        val targetColumns: Seq[String] =
          if (targetSchema.nonEmpty) targetSchema.map(_.name)
          else sourceTableColumns
        val missingColumnSchema: Seq[StructField] =
          targetSchema.filterNot(struct => {
            sourceTableColumns.contains(struct.name.toLowerCase)
          })

        validateMissingTargetColumn(
          tableConf.source_system,
          sourceTableColumns,
          missingColumnSchema.map(_.name)
        )

        val modifiedDf: DataFrame =
          defaultValuesForMissingColumns(dataDF, missingColumnSchema)
            .select(targetColumns.head, targetColumns.tail: _*)

        val targetDataFrame: DataFrame = targetSchema
          .map(structType => (structType.name, structType.dataType))
          .foldLeft(modifiedDf)((df, schema) => {
            df.withColumn(schema._1, col(schema._1).cast(schema._2))
          })

        // val targetDataFrame : dataFrame = cleanDataFrame(typedDf)

        logger.info(
          s"Data type casting completed for table ${tableConf.target_table}"
        )
        targetDataFrame
      }
    })
  }

  private def validateMissingTargetColumn(source_system: String,
                                          sourceTableColumns: Seq[String],
                                          missingColumns: Seq[String]): Unit = {
    if (missingColumns.nonEmpty && !isSourceEnabled(
          skipMissingTargetColumn,
          source_system
        )) {
      logger.info(
        s"Source column names are ${sourceTableColumns.mkString(StringExpr.comma)}"
      )
      throw new Exception(
        s"Missing target column names in source dataset : ${missingColumns
          .mkString(StringExpr.comma)}"
      )
    } else if (missingColumns.nonEmpty) {
      logger
        .warn(
          s"Missing mapping columns from source are ${missingColumns.mkString(StringExpr.comma)}"
        )
    }
  }

}
