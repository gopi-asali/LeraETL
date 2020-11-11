package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.lera.TableConfig
import org.lera.etl.util.Constants._
import org.lera.etl.util.utils._

import scala.collection.parallel.ParSeq

object JoinTransformer extends JoinBaseTransformer {

  private val logger: Logger = Logger.getLogger(JoinTransformer.getClass)

  /*
   * Each implementation provides the different transformation logic based on sources
   * @param dataFrameSeq tuples of table config and dataFrame
   * @return
   *
   * */

  override def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)] = {

    val joinConfigDataFrame: DataFrame =
      configHandler(dataFrameSeq.keys)(getTableConfigFunc(joinConfigTableName))

    dataFrameSeq.flatMap(dataTup => {
      val (tableConf, sourceDf) = dataTup
      errorHandler(tableConf) {
        val getJoinMap: Seq[(JoinTable, Map[String, String])] =
          getJoiningTableDetails(tableConf, joinConfigDataFrame)
        JoinTransformation(tableConf, sourceDf, getJoinMap)
      }
    })
  }

  def JoinTransformation(
    tableConf: TableConfig,
    sourceDf: DataFrame,
    getJoinMap: Seq[(JoinTable, Map[String, String])]
  ): DataFrame = {
    getJoinMap.foldLeft(sourceDf)(
      (df: DataFrame, joinInfo: (JoinTable, Map[String, String])) => {
        logger.info(
          s"joining lookup tables >> ${tableConf.source_table} with ${joinInfo._1.lookupTable}"
        )

        val lookupConfig: TableConfig =
          joinInfo._1.getModifiedTableConf(tableConf)

        val sourceDf: DataFrame = nullReplaceDf(df)(joinInfo._2.keys.toSeq)
        val lookupDf: DataFrame =
          nullReplaceDf(readLookUpData(lookupConfig))(joinInfo._2.values.toSeq)

        val seqCon: Column = conditionCreator(sourceDf)(lookupDf)(joinInfo._2)

        logger.info(
          s"Join condition for tables ${tableConf.source_table} and ${joinInfo._1.lookupTable} is $seqCon"
        )

        val dropColumns: Seq[Column] = getDropColumns(sourceDf)(lookupDf)
        logger.info(s"Delete columns are $dropColumns")
        val joinedDf = sourceDf.join(lookupDf, seqCon, joinInfo._1.joinType)
        dropColumns.foldLeft(joinedDf)((df, column) => df.drop(column))
      }
    )
  }

  /*
   * @param tableConf table configuration
   * @return
   *
   * */

  def getJoiningTableDetails(
    tableConf: TableConfig,
    configDataFrame: DataFrame
  ): Seq[(JoinTable, Map[String, String])] = {
    configDataFrame.show(truncate = false)
    val whereQuery: String =
      Map(
        sourceDB -> tableConf.source_database,
        sourceTable -> tableConf.source_table,
        targetDB -> tableConf.target_database,
        targetTable -> tableConf.target_table
      ).toWhereCondition.ignoreCaseInSQL

    val joinTableDf: Array[JoinLookup] = configDataFrame
      .where(whereQuery)
      .select(
        lookupDB,
        lookupTable,
        sourceColumn,
        lookupColumn,
        joinType,
        joinOrder,
        lookupTableType
      )
      .as[JoinLookup](Encoders.product[JoinLookup])
      .collect()

    joinTableDf
      .groupBy(_.getJoinTable)
      .mapValues(
        _.map(lookup => (lookup.source_column, lookup.lookup_column)).toMap
      )
      .toSeq
      .sortBy(_._1.joinOrder)
  }

}

case class JoinLookup(lookup_database: String,
                      lookup_table: String,
                      lookup_table_type: String,
                      source_column: String,
                      lookup_column: String,
                      join_type: String,
                      join_order: Int) {
  def getJoinTable: JoinTable = {
    JoinTable(
      lookup_database,
      lookup_table,
      lookup_table_type,
      join_type,
      join_order
    )
  }
}

case class JoinTable(lookupDatabase: String,
                     lookupTable: String,
                     lookupTableType: String,
                     joinType: String,
                     joinOrder: Int) {
  def getModifiedTableConf(tableConfig: TableConfig): TableConfig = {
    tableConfig.copy(
      source_database = lookupDatabase,
      source_table = lookupTable,
      source_table_type = lookupTableType,
      file_path = lookupTable,
      load_type = fullLoadType
    )
  }
}
