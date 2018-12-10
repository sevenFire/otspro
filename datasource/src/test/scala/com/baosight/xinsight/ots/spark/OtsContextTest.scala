package com.baosight.xinsight.ots.spark

import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
//import org.apache.spark.Logging

object OtsContextTest {
  def main(args: Array[String]) {
    println("========Test OtsContextTest==========")
    val tableName = "test"
    var startKey = "0000" //hex
    var stopKey = "ffff" //hex

    try {
      startKey = Bytes.toHex(PrimaryKeyUtil.buildPrimaryKey("0000", PrimaryKeyUtil.TYPE_STRING, "", PrimaryKeyUtil.TYPE_STRING)) //hex
      stopKey = Bytes.toHex(PrimaryKeyUtil.buildPrimaryKey("ffff", PrimaryKeyUtil.TYPE_STRING, "", PrimaryKeyUtil.TYPE_STRING)) //hex
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    }

    val conf = HBaseConfiguration.create()
    conf.set(OtsSparkConf.USER_NAME, "admin@root")
    conf.set(OtsSparkConf.USER_PASSWORD, "admin")

    conf.set(OtsSparkConf.OTS_TABLE_NAME, tableName)
    conf.set(OtsSparkConf.OTS_COLUMNS, "column1:Int,column2:String")
    conf.set(OtsSparkConf.OTS_START_KEY, startKey)
    conf.set(OtsSparkConf.OTS_STOP_KEY, stopKey)

    val sparkConf = new SparkConf().setAppName("OtsContextTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val otsContext = new OtsContext(sc, conf)
    val rdd = otsContext.rdd
    rdd.foreach(key => println("#########" + key.toString))

    val sqlContext: SQLContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(otsContext.rowRDD, otsContext.otsSchema)
    df.registerTempTable("tmp_table")
    println(df.schema.treeString)
    println(df.columns.mkString(","))
    //sqlContext.sql("SELECT * FROM tmp_table limit 5").show()
    sqlContext.sql("SELECT * FROM tmp_table limit 2 ").show()

    val df2 = otsContext.dataFrame
    println(df2.schema.treeString)
    println(df2.columns.mkString(","))
    //df2.registerTempTable("tmp_table2")
    //df2.sqlContext.sql("select * from tmp_table2 limit 5").show()

    println("++++++++++++++++")
    println(otsContext.otsSchema.treeString)
    println(otsContext.otsSchema.mkString(","))

    sc.stop()
  }
}
