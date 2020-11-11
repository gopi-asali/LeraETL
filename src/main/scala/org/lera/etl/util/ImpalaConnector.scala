package org.lera.etl.util
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.lera.etl.util.utils.JDBC_URL_Generator

import scala.util.{Failure, Success}
import org.apache.log4j.Logger
import org.lera.ContextCreator
import org.lera.etl.util.Constants._
import scala.util.Try

object ImpalaConnector extends ContextCreator{

  lazy val connection: Connection = DriverManager.getConnection(connectionURL)
  lazy val statement: Statement = connection.createStatement()

 /* val userName : String = getProperty(impalaUserName)
  val password : String = getProperty(impalaPassword)
  val JDBCDriver : String = getProperty(jdbcDriver)
*/

  val userName : String = getProperty(hiveUserName)
  val password : String = getProperty(hivePassword)
  val JDBCDriver : String = getProperty(hiveJDBCDriver)

  Class.forName(JDBCDriver).newInstance

  //val connectionURL: String = JDBC_URL_Generator(getProperty(impalaURL), userName,password)
  //val connectionURL: String = JDBC_URL_Generator(getProperty(hiveURL), userName, password)

  val connectionURL: String = JDBC_URL_Generator(getProperty(hiveURL), userName, password)

  private val logger: Logger = Logger.getLogger(ImpalaConnector.getClass)

  /*
  *Driver properties to initialize kudu table connection
  *
  */

  def getImpalaProperty: Properties = {
    val prob = new Properties
    prob.setProperty(driver,JDBCDriver)
    prob
  }

  /*
  *Generates delete table query to delete data from kudu table
  *
  *@param tableName impala table name which data need to be dropped
  *@return
  */

  def buildDeleteStatement(tableName: String, whereCond : String = StringExpr.empty) : String = {
    val cond =
      if(whereCond.isEmpty) StringExpr.empty
      else s"where $whereCond"

    s"DELETE $tableName $cond;"
  }

  /*
  *Build upsert query to upsert data from hive intermediate table into kudu tables
  *
  *@param srcHiveTableName hive intermediate table name
  *@param targetKuduTableName target kudu table name
  *@param columns column Names
  *
  */

  def buildQueryForInsert(
                           srcHiveTableName : String,
                           targetKuduTableName : String,
                           columns : Array[String]
                         )(isFullLoad: Boolean = true) : String = {
    val selectColumns =
      columns.map(column => s"'$column'.").mkString(StringExpr.comma)
    val insertType : String =
      if(isFullLoad) "INSERT"
      else "UPSERT"

    s"$insertType INTO TABLE $targetKuduTableName($selectColumns) SELECT $selectColumns FROM $srcHiveTableName;"
  }

  /*
  *Executes sql query in impala shell
  *
  *@param queries prepared query statement
  **/

  def executeQuery(queries : String*): Boolean = {
    logger.info(s"Executing query in Impala:: $queries")
    Try {
      queries.foreach(query => {
        println(s"Executing query $query")
        statement.execute(query)
      })
    } match {
      case Success(_) => true
      case Failure(exception) =>
        logger.error(
          s"Impala query execution has failed due to ${exception.getMessage}"
        )
        throw exception
    }
  }
}