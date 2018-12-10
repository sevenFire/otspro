package com.baosight.xinsight.ots.spark

object OtsSparkConf {
  // This is the ots-spark configuration. User can either set them in SparkConf, which
  // will take effect globally, or configure it when used, which will overwrite the value
  // set in SparkConf. If not set, the error exception got.
  val OTS_TABLE_NAME: String = "ots.table.name"
  val OTS_START_KEY: String = "ots.start.key"
  val OTS_STOP_KEY: String = "ots.stop.key"
  val OTS_COLUMNS: String = "ots.columns"

  val USER_NAME: String = "ots.user.name"
  val USER_PASSWORD: String = "ots.user.password"

  val OTS_COLUMN_TYPE_BOOLEAN: String = "BOOLEAN"
  val OTS_COLUMN_TYPE_INT: String = "INT"
  val OTS_COLUMN_TYPE_LONG: String = "LONG"
  val OTS_COLUMN_TYPE_FLOAT: String = "FLOAT"
  val OTS_COLUMN_TYPE_DOUBLE: String = "DOUBLE"
  val OTS_COLUMN_TYPE_STRING: String = "STRING"
  val OTS_COLUMN_TYPE_TIMESTAMP: String = "TIMESTAMP"
  val OTS_COLUMN_TYPE_BINARY: String = "BINARY"

  private[ots] val MODULE_NAME: String = "ots"

  private[ots] val TABLE_COLUMN_HASHKEY: String = "hash_key"
  private[ots] val TABLE_COLUMN_RANGEKEY: String = "range_key"
}
