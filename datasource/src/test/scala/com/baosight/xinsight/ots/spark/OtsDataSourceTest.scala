package com.baosight.xinsight.ots.spark

import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object OtsDataSourceTest {
  def main(args: Array[String]) {
    println("========Test OtsDataSourceTest==========")
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

    val options: Map[String, String] = Map(
      OtsSparkConf.USER_NAME -> "admin@root",
      OtsSparkConf.USER_PASSWORD -> "admin",
      OtsSparkConf.OTS_COLUMNS -> "column1:Int,column2:String",
      OtsSparkConf.OTS_TABLE_NAME -> tableName,
      OtsSparkConf.OTS_START_KEY -> startKey,
      OtsSparkConf.OTS_STOP_KEY -> stopKey
    )

    val sparkConf = new SparkConf().setAppName("OtsDataSourceTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val reader = sqlContext.read
    val readerOp = reader.options(options)
    val readerFormat = readerOp.format("com.baosight.xinsight.ots.spark")
    val df0 = readerFormat.load()
    df0.limit(2).show()

    val df1 = sqlContext.read.options(options).format("com.baosight.xinsight.ots.spark").load()
    df1.registerTempTable("sql_temp")
    sqlContext.sql("select * from sql_temp limit 2").show()

    df1.limit(1).show()

    sc.stop()
  }
}
