package org.lera.etl.util

object Constants {
val stringNULL="null"
  val fullLoadType: String = "full"
  val incremental: String = "incremental"
  val incr: String = "incr"

  val csv: String = "csv"
  val sourceSystem: String = "source_system"
  val sourceDataRegionName: String = "sourcedata_regionName"


  val partitionColumn: String = ""
  val numOfPartitions: String = ""

  val sourceDB: String = "source_database"
  val targetDB: String = "target_database"

  val configDB: String = "spark.config_database"
  val auditDB: String = "spark.audit_database"
  val genericCfgTableName: String = "spark.generic_config"
  val columnMappingCfgTableName: String = "spark.column_mapping_table"
  val auditCfgTableName: String = "spark.audit"
  val defaultValuesCfgTableName: String = "spark.default_values_table"
  val joinCfgTableName: String = "spark.join_table"
  val lookupCfgTableName: String = "spark.generic_lookup_table"
  val conditionCfgTableName: String = "spark.condition_table"
  val deleteCfgTableName: String = "spark.delete_table"
  val filterCfgTableName: String = "spark.filter_table"
  val partitionCfgTableName: String = "spark.partition_table"
  val skipMissingTargetColumn: String = "spark.skip_missing_target_column_source_"
  val skipMissingSourceColumn: String =  "spark.skip_missing_source_column_source_"

  val concatChar: String = "_"
  val sourceTable: String = "source_table"
  val targetTable: String  = "target_table"
  val filePath: String = "file_path"
  val sourceType: String = "source_type"
  val targetType: String =  "target_type"
  val loadType: String = "load_type"
  val sourceIncrementColumn: String = "source_increment_column"
  val targetIncrementColumn: String = "target_increment_column"
  val sourceColumn: String = "source_column"
  val targetColumn: String = "target_column"
  val defaultValue: String = "default_value"
  val sheetName: String = "spark.excel_sheet_name"

  //join table columns
  val lookupDB: String = ""
  val lookupTable: String = ""
  val lookupColumn: String = ""
  val joinType: String = ""
  val joinOrder: String = ""
  val lookupOrder: String = ""
  val lookupTableType: String = ""
  val sqlAND: String = " AND "
  val sqlOR: String = " OR "


  val database_name: String = ""
  val tableName: String = ""
  val partitionColumnName: String = ""
  val partitionColumnType: String = ""
  val numberOfPartitions: String = ""
  val lowerBound: String = ""
  val upperBound: String = ""


  val aggMaxColumn: String = "agg_max_value"

  val userName: String = "<username>"
  val password: String = "<password>"
  val hdfsFileLocationPrefix: String = ""

  val sparkAppName: String = "spark.app.name"
  val loggerLevel: String = "spark.log_level"

  //val impalaURL: String = "spark.impalaConnectionURL"
  val hiveURL: String = "spark.hiveConnectionURL"

  //val jdbcDriver: String = "spark.impalaJDBCDriver"
  val hiveJDBCDriver: String = "spark.hiveJDBCDriver"

  //val impalaUserName: String = "spark.impala_username"
  //val impalaPassword: String = "spark.impala_password"

  val hiveUserName: String = "spark.hive_username"
  val hivePassword: String = "spark.hive_password"

  val driver: String = "driver"
  val kuduType: String = "Kudu"
  val hiveType: String = "Hive"

  //Staging table will be suffixed with _stg to the source table name
  val stagingIdentifier: String = "_stg"

  val typeCast: String = "TypeCast"
  val status: String = "status"
  val inActive: String = "inactive"
  val rowNumber: String = "row_num"
  val monthYear: String             = "month_year"
  val monthYearSourceFormat: String = "yyyyMM"
  val monthYearTargetFormat: String = "MMM-yyyy"
  val fiscalYear: String            = "fiscal_year"
  val fiscalWeekOfYear: String      = "fiscal_week_year"
  val fiscalYearFormat: String      = "yyyy"
  val weekOfMonth: String           = "week_of_month"
  val weekOfYear: String            = "week_of_year"
  val loadDate: String              = "loaddate"
  val yyyymm: String                = "yyyymm"
  val fiscal_year: String           = "fiscal_year"
  val month_year: String            = "month_year"
  val fiscal_timestamp: String      = "fiscal_timestamp"


  object StringExpr {

    def empty: String = ""
    def line: String = "\n"
    def plus : String = "\\+"
    def startBrace: String = "("
    def endBrace: String = ")"
    def space: String = " "
    def pipe: String = "|"
    def comma: String = ","
    def quote: String = "'"
    def equal: String = "="
    def notEqual: String = "!="
    def ques: String = "?"
    def hyphen: String = "-"
    def dot: String = "\\."
    def slash: String = "\" "
    def underScore: String = "_"
  }

  implicit
  class StringImplicits(consValue: String) {
    def ignoreCaseInSQL: String = {
      val equalReplacer: String = "))=LOWER(TRIM("
      val notEqualReplacer: String = "))!=LOWER(TRIM("
      val sqlAndReplacer: String = ")) AND LOWER(TRIM("
      val sqlOrReplacer: String = ")) OR LOWER(TRIM("
      s"LOWER(TRIM($consValue))".replace(StringExpr.equal, equalReplacer)
        .replace(StringExpr.notEqual, notEqualReplacer)
        .replace(sqlAND, sqlAndReplacer).replace(sqlOR, sqlOrReplacer)
    }
  }

}
