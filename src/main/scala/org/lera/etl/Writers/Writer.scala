package org.lera.etl.Writers

import org.apache.spark.sql.DataFrame
import org.lera.{ContextCreator, TableConfig}
import org.lera.etl.util.Constants._

import scala.collection.parallel.ParSeq


/*
 * Writer for persisting dataset into target system
 *
 *
 *  */

trait Writer extends ContextCreator{

  /*
   * Write datasets into target tables or file system
   *
   * @param dataSet tuple or dataSet and table properties
   *
   * */

  def write(dataSet: ParSeq[(TableConfig, DataFrame)])

  protected def getSourceSystemNames(df: DataFrame): Array[String] = {
    df.selectExpr(sourceSystem)
      .distinct
      .collect
      .map(_.get(0))
      .filter(_ != null)
      .map(_.toString.trim.toLowerCase)
  }

}
