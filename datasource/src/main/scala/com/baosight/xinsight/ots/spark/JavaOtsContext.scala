package com.baosight.xinsight.ots.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

/**
  * This is the Java Wrapper over HBaseContext which is written in
  * Scala.  This class will be used by developers that want to
  * work with Spark or Spark Streaming in Java
  *
  * @param jsc    This is the JavaSparkContext that we will wrap
  * @param config This is the config information to out HBase cluster
  */
class JavaOtsContext(@transient jsc: JavaSparkContext,
                     @transient config: Configuration) extends Serializable {
  val otsContext = new OtsContext(jsc.sc, config)

  /**
    * Get JavaRDD[Row] with input [[config]] init
    *
    * @return JavaRDD[org.apache.spark.sql.Row]
    */
  def rowRDD: JavaRDD[org.apache.spark.sql.Row] = {
    JavaRDD.fromRDD(otsContext.rowRDD)
  }

  /**
    * Get JavaRDD[Row] with given parameters
    *
    * @param tableName ots table name
    * @param startKey  coded with hex string
    * @param stopKey   coded with hex string
    * @return JavaRDD[org.apache.spark.sql.Row]
    */
  def rowRDD(tableName: String, startKey: String, stopKey: String): JavaRDD[org.apache.spark.sql.Row] = {
    JavaRDD.fromRDD(otsContext.rowRDD(tableName, startKey, stopKey))
  }

  def otsColumns: java.util.List[String] = otsSchema.fields.map(_.name).toList

  def otsSchema: StructType = otsContext.otsSchema

  /**
    * Get data frame with input [[config]] init
    *
    * @return DataFrame
    */
  def dataFrame: DataFrame = otsContext.dataFrame

  /**
    * Get data frame with given parameters
    *
    * @param tableName ots table name
    * @param startKey  coded with hex string
    * @param stopKey   coded with hex string
    * @return DataFrame
    */
  def dataFrame(tableName: String, startKey: String, stopKey: String): DataFrame = {
    otsContext.dataFrame(tableName, startKey, stopKey)
  }

  /**
    * Get JavaRDD[OtsRowRecord] with input [[config]] init
    *
    * @return JavaRDD[OtsRowRecord]
    */
  def rdd: JavaRDD[OtsRowRecord] = {
    JavaRDD.fromRDD(otsContext.rdd)
  }

  /**
    * Get JavaRDD[OtsRowRecord] with given parameters
    *
    * @param tableName ots table name
    * @param startKey  coded with hex string
    * @param stopKey   coded with hex string
    * @return JavaRDD[OtsRowRecord]
    */
  def rdd(tableName: String, startKey: String, stopKey: String): JavaRDD[OtsRowRecord] = {
    JavaRDD.fromRDD(otsContext.rdd(tableName, startKey, stopKey))
  }

  /**
    * save original rdd data into ots.
    *
    * support for a user to take RDD
    * and generate puts and send them to ots.
    *
    * @param rdd       RDD with data to write into ots
    */
  def saveRdd(rdd: RDD[OtsRowRecord]) {
    otsContext.saveRdd(rdd)
  }

  def saveRdd(rdd: RDD[OtsRowRecord], tableName: String) {
    otsContext.saveRdd(rdd, tableName)
  }

  /**
    * save Row rdd data into ots.
    *
    * support for a user to take RDD
    * and generate puts and send them to ots.
    *
    * @param rowRdd       RDD with data to write into ots
    */
  def saveRowRdd(rowRdd: RDD[org.apache.spark.sql.Row]) {
    otsContext.saveRowRdd(rowRdd)
  }
}
