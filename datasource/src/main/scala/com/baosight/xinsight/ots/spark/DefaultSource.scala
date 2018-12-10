package com.baosight.xinsight.ots.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
//import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * DefaultSource for integration with Spark's dataframe datasources.
  * This class will produce a relationProvider based on input given to it from spark
  *
  * Spark will automatically look for a [[RelationProvider]] implementation named
  * `DefaultSource` when the user specifies the path of a source during DDL
  * operations through [[org.apache.spark.sql.DataFrameReader.format]].
  */
class DefaultSource extends RelationProvider with Logging {

  val OTS_DEDUCE_CONFIG_RESOURCES: String = "ots.deduce.config.resources"

  /**
    * Is given input from SparkSQL to construct a BaseRelation
    *
    * @param sqlContext SparkSQL context
    * @param parameters Parameters given to us from SparkSQL
    * @return A BaseRelation Object
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val tableName = parameters.getOrElse(OtsSparkConf.OTS_TABLE_NAME, "").trim
    if (tableName.isEmpty)
      new IllegalArgumentException("Invalid value for " + OtsSparkConf.OTS_TABLE_NAME + " '" + tableName + "'")

    lazy val columns: String = {
      val tmpColumns = parameters.getOrElse(OtsSparkConf.OTS_COLUMNS, "").trim
      if (tmpColumns.isEmpty)
        throw new IllegalArgumentException("Invalid value for " + OtsSparkConf.OTS_COLUMNS + " '" + tmpColumns + "'")
      tmpColumns
    }
    //@transient
    //val format: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss.SSS")

    val startKey = parameters.getOrElse(OtsSparkConf.OTS_START_KEY, "").trim
    val stopKey = parameters.getOrElse(OtsSparkConf.OTS_STOP_KEY, "").trim

    val userName: String = parameters.getOrElse(OtsSparkConf.USER_NAME, "")
    if (userName.isEmpty)
      new IllegalArgumentException("Invalid value for " + OtsSparkConf.USER_NAME + " '" + userName + "'")

    val password: String = parameters.getOrElse(OtsSparkConf.USER_PASSWORD, "")
    if (password.isEmpty)
      new IllegalArgumentException("Invalid value for " + OtsSparkConf.USER_PASSWORD + " '" + password + "'")

    val useDeduceConfigResources = parameters.getOrElse(OTS_DEDUCE_CONFIG_RESOURCES, "true")

    new TsdbRelation(tableName, startKey, stopKey, columns, userName, password,
      useDeduceConfigResources.equalsIgnoreCase("true"))(sqlContext)
  }
}

/**
  * Implementation of Spark BaseRelation that will build up tsdb logic
  *
  * @param tableName                table name under collector
  * @param startKey                 query start key, formatted with hex
  * @param stopKey                  query stop key, formatted with hex
  * @param userName                 username of xinsight platform
  * @param password                 password of xinsight platform
  * @param useDeduceConfigResources whether to deduce config of hadoop environment
  * @param sqlContext               SparkSQL context
  */
class TsdbRelation(val tableName: String,
                   val startKey: String,
                   val stopKey: String,
                   val columns: String,
                   val userName: String,
                   val password: String,
                   val useDeduceConfigResources: Boolean)(
                    @transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with Serializable with Logging {

  private val otsContext: OtsContext = {

    @transient
    val config = HBaseConfiguration.create()
    config.reloadConfiguration()

    config.set(OtsSparkConf.USER_NAME, userName)
    config.set(OtsSparkConf.USER_PASSWORD, password)

    config.set(OtsSparkConf.OTS_COLUMNS, columns)
    config.set(OtsSparkConf.OTS_TABLE_NAME, tableName)
    config.set(OtsSparkConf.OTS_START_KEY, startKey)
    config.set(OtsSparkConf.OTS_STOP_KEY, stopKey)

    if (useDeduceConfigResources) {
      config.addResource("/etc/hbase/conf/hbase-site.xml")
    }

    new OtsContext(sqlContext.sparkContext, config)
  }

  /**
    * Generates a Spark SQL schema object so Spark SQL knows what is being
    * provided by this BaseRelation
    *
    * @return schema
    */
  override def schema: StructType = otsContext.otsSchema

  /**
    * Build the RDD to scan rows.
    *
    * @param requiredColumns columns that are being requested by the requesting query
    * @return RDD will all the results from Tsdb
    */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    otsContext.rdd.map(r => {
      Row.fromSeq(requiredColumns.map(col => otsContext.getColumnValue(col, r)))
    })
  }
}
