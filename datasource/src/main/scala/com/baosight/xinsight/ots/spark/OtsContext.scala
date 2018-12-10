package com.baosight.xinsight.ots.spark

import com.baosight.xinsight.auth.AuthManager
import com.baosight.xinsight.config.{ConfigConstants, ConfigReader}
import com.baosight.xinsight.model.UserInfo
import com.baosight.xinsight.ots.OtsConstants
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HConstants, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

import scala.collection.{JavaConversions, mutable}
import scala.reflect.ClassTag

/**
  * OtsContext is a facade for Ots operations
  */
class OtsContext(@transient sc: SparkContext,
                 @transient val config: Configuration)
  extends Serializable with Logging {

  lazy val tableName: String = {
    val tmpTableName = config.get(OtsSparkConf.OTS_TABLE_NAME, "").trim
    if (tmpTableName.isEmpty)
      throw new IllegalArgumentException("Invalid value for " + OtsSparkConf.OTS_TABLE_NAME + " '" + tmpTableName + "'")
    tmpTableName
  }

  lazy val requiredColumns: String = config.get(OtsSparkConf.OTS_COLUMNS, "").trim
  if (requiredColumns.isEmpty)
    throw new IllegalArgumentException("Invalid value for " + OtsSparkConf.OTS_COLUMNS + " '" + requiredColumns + "'")

  lazy val columns: mutable.LinkedHashMap[String, SchemaQualifierDefinition] = {
    generateSchemaMappingMap(requiredColumns)
  }

  @transient
  private lazy val configReader: ConfigReader = new ConfigReader(OtsSparkConf.MODULE_NAME)
  private lazy val user: UserInfo = {
    val aasServer = configReader.getValue(ConfigConstants.AAS_HOST, "127.0.0.1:80")
    val aasService = configReader.getValue(ConfigConstants.AAS_REST_SERVICE_NAME, "aas")
    val sb = new StringBuilder
    sb.append(aasServer).append("/").append(aasService)

    val token = AuthManager.login(sb.toString, userName, password)
    if (null == token)
      throw new IllegalArgumentException("Failed to login, username = " + userName + ", password = " + password)

    val user = AuthManager.getUserInfo(sb.toString, token)
    if (null == user)
      throw new IllegalArgumentException("Failed to check user info, username = " + userName + ", password = " + password)
    user
  }
  private lazy val meta = new MetaHelper({
    val postgres_quorum: String = configReader.getValue(ConfigConstants.POSTGRESQL_QUORUM, "127.0.0.1:5432")
    var postgres_server: String = "127.0.0.1"
    var postgres_port: String = "5432"
    if (null != postgres_quorum) {
      val postgres_hosts: Array[String] = postgres_quorum.split(",")
      if (postgres_hosts.length > 0) {
        val postgres_host: Array[String] = postgres_hosts(0).split(":")
        if (postgres_host.length > 0) {
          postgres_server = postgres_host(0)
          if (postgres_host.length > 1) postgres_port = postgres_host(1)
        }
      }
    }
    val sb = new StringBuilder
    sb.append(postgres_server).append(":").append(postgres_port).toString()
  },
    configReader.getValue(ConfigConstants.POSTGRESQL_USER, "postgres"),
    configReader.getValue(ConfigConstants.POSTGRESQL_PASSWORD, "q1w2e3"),
    configReader.getValue(ConfigConstants.OTS_DBNAME, "ots"))
  private lazy val table: OtsTable = {
    val queryTable = meta.getTableByName(user.getTenantId, tableName)
    if (null == queryTable)
      throw new IllegalArgumentException("Failed to query table info, tableName = " + tableName)
    queryTable
  }
  private lazy val hbaseContext: HBaseContext = new HBaseContext(sc, config)

  config.set(HConstants.ZOOKEEPER_QUORUM, configReader.getValue(ConfigConstants.ZOOKEEPER_QUORUM, "127.0.0.1:2181"))
  config.set(HConstants.ZK_SESSION_TIMEOUT, configReader.getValue(ConfigConstants.ZOOKEEPER_TIMEOUT, "3000"))
  val userName: String = config.get(OtsSparkConf.USER_NAME, "")
  if (userName.isEmpty)
    throw new IllegalArgumentException("Invalid value for " + OtsSparkConf.USER_NAME + " '" + userName + "'")
  val password: String = config.get(OtsSparkConf.USER_PASSWORD, "")
  if (password.isEmpty)
    throw new IllegalArgumentException("Invalid value for " + OtsSparkConf.USER_PASSWORD + " '" + password + "'")

  //@transient
  //private val format: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss.SSS")
  val startKey: String = config.get(OtsSparkConf.OTS_START_KEY, "").trim
  val stopKey: String = config.get(OtsSparkConf.OTS_STOP_KEY, "").trim

  /**
    * Get RDD[Row] with input [[config]] init
    *
    * @return RDD[org.apache.spark.sql.Row]
    */
  def rowRDD: RDD[org.apache.spark.sql.Row] = rowRDD(tableName, startKey, stopKey)

  private[ots] def rowRDD(tableName: String, startKey: String, stopKey: String): RDD[org.apache.spark.sql.Row] = {
    rdd(tableName, startKey, stopKey).map(r => {
      Row.fromSeq(columns.keySet.toArray.map(col => getColumnValue(col, r)))
    })
  }

  private[ots] def getColumnValue(columnName: String, rec: OtsRowRecord): Any = {
    val columnDef = columns.get(columnName)
    if (columnDef == null)
      throw new IllegalArgumentException("Unknown column:" + columnName)

    var cellByteValue: Array[Byte] = null
    if (columnDef.get.columnFamilyBytes.isEmpty) {
      if (columnName.equals(OtsSparkConf.TABLE_COLUMN_HASHKEY)) {
        cellByteValue = PrimaryKeyUtil.getHashKey(rec.getRowkey)
      } else {
        cellByteValue = PrimaryKeyUtil.getRangeKey(rec.getRowkey)
      }
    } else {
      cellByteValue = rec.getCellMap(columnName).getValue
    }

    if (cellByteValue == null) null
    else columnDef.get.columnSparkSqlType match {
      case BooleanType => Bytes.toBoolean(cellByteValue)
      case IntegerType => java.lang.Integer.parseInt(Bytes.toString(cellByteValue))
      case LongType => java.lang.Long.parseLong(Bytes.toString(cellByteValue))
      case FloatType => java.lang.Float.parseFloat(Bytes.toString(cellByteValue))
      case DoubleType => java.lang.Double.parseDouble(Bytes.toString(cellByteValue))
      case StringType => Bytes.toString(cellByteValue)
      case TimestampType => java.lang.Long.parseLong(Bytes.toString(cellByteValue))
      case BinaryType => Bytes.toHex(cellByteValue) //hex
      case _ => Bytes.toString(cellByteValue)
    }
  }

  def otsColumns: Array[String] = otsSchema.fields.map(_.name)

  /**
    * Get data frame with input [[config]] init
    *
    * @return DataFrame
    */
  def dataFrame: DataFrame = dataFrame(table.getName, startKey, stopKey)

  private[ots] def dataFrame(tableName: String, startKey: String, stopKey: String): DataFrame = {
    val sqlContext: SQLContext = new SQLContext(sc)
    sqlContext.createDataFrame(rowRDD(tableName, startKey, stopKey), otsSchema)
  }

  def otsSchema: StructType = {
    val structFieldArray = new Array[StructField](columns.size)
    var indexCounter = 0

    val _hash_key = StructField(OtsSparkConf.TABLE_COLUMN_HASHKEY, table.getHashkeyType match {
      case PrimaryKeyUtil.TYPE_NUMBER => LongType
      case PrimaryKeyUtil.TYPE_STRING => StringType
      case PrimaryKeyUtil.TYPE_BINARY => BinaryType //hex
    }, nullable = false)
    structFieldArray(indexCounter) = _hash_key
    indexCounter += 1

    if (table.getKeytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
      val _range_key = StructField(OtsSparkConf.TABLE_COLUMN_RANGEKEY, table.getRangekeyType match {
        case PrimaryKeyUtil.TYPE_NUMBER => LongType
        case PrimaryKeyUtil.TYPE_STRING => StringType
        case PrimaryKeyUtil.TYPE_BINARY => BinaryType //hex
      }, nullable = false)
      structFieldArray(indexCounter) = _range_key
      indexCounter += 1
    }

    for ((_, v: SchemaQualifierDefinition) <- columns) {
      if (!v.columnFamily.isEmpty) {
        val _column = StructField(v.columnName, v.columnSparkSqlType)
        structFieldArray(indexCounter) = _column
        indexCounter += 1
      }
    }

    val result = new StructType(structFieldArray)
    result
  }

  private[ots] def readResult(rec: Result): OtsRowRecord = {
    val recModel = new OtsRowRecord
    var rowKey = false

    /*
    for ((_, v:SchemaQualifierDefinition) <- columns) {
      if (!v.columnFamily.isEmpty) {
        if (!rowkey) {
          recModel.setRowkey(rec.getRow)
          rowkey = true
        }

        val cellModel = new OtsRowCell
        cellModel.setQualifier(v.qualifier)
        cellModel.setValue(rec.getValue(Bytes.toBytes(v.columnFamily), Bytes.toBytes(v.qualifier)))
        //cellModel.setTimestamp(rec.getTimestamp)
        recModel.addCell(cellModel)
      }
    }*/

    for (cell <- rec.rawCells) {
      if (cell != null) {
        if (!rowKey) {
          recModel.setRowkey(CellUtil.cloneRow(cell))
          rowKey = true
        }

        val cellModel = new OtsRowCell
        cellModel.setQualifier(Bytes.toString(CellUtil.cloneQualifier(cell)))
        cellModel.setValue(CellUtil.cloneValue(cell))
        cellModel.setTimestamp(cell.getTimestamp)
        recModel.addCell(cellModel)
      }
    }
    recModel
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
    hbaseContext.bulkPut[OtsRowRecord](rdd, TableName.valueOf(this.tableName),
      f = (putOtsRecord) => {
        if (table.getKeytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
          if (null == putOtsRecord.getRangekey || putOtsRecord.getRangekey.isEmpty)
            throw new IllegalArgumentException("invalid ots record, lack of range key for table = " + tableName)
        }
        putOtsRecord.toPut
      })
  }

  def saveRdd(rdd: RDD[OtsRowRecord], tableName: String) {
    val queryTable = meta.getTableByName(user.getTenantId, tableName)
    if (null == queryTable)
      throw new IllegalArgumentException("Failed to query table info, tableName = " + tableName)

    hbaseContext.bulkPut[OtsRowRecord](rdd, TableName.valueOf(getOtsTable(user.getTenantId, tableName)),
      (putOtsRecord) => {
        if (queryTable.getKeytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
          if (null == putOtsRecord.getRangekey || putOtsRecord.getRangekey.isEmpty)
            throw new IllegalArgumentException("invalid ots record, lack of range key for table = " + tableName)
        }
        putOtsRecord.toPut
      })
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
    val recRdd = rowRdd.map(row => {
      val _hash_key_idx = row.fieldIndex(OtsSparkConf.TABLE_COLUMN_HASHKEY)
      if (_hash_key_idx < 0)
        throw new IllegalArgumentException("Invalid schema for input row rdd, lack hash key")

      val rec:OtsRowRecord = new OtsRowRecord

      val _range_key_idx = row.fieldIndex(OtsSparkConf.TABLE_COLUMN_RANGEKEY)
      if (this.table.getKeytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
        if (_range_key_idx < 0)
          throw new IllegalArgumentException("Invalid schema for input row rdd, lack range key")

        rec.setRowkey(PrimaryKeyUtil.buildPrimaryKey(row.getString(_hash_key_idx), this.table.getHashkeyType, row.getString(_range_key_idx), this.table.getRangekeyType))
      } else {
        rec.setRowkey(PrimaryKeyUtil.buildPrimaryKey(row.getString(_hash_key_idx), this.table.getHashkeyType))
      }

      for (column:String <- row.schema.fieldNames) {
        if (!column.equals(OtsSparkConf.TABLE_COLUMN_HASHKEY) && !column.equals(OtsSparkConf.TABLE_COLUMN_RANGEKEY)) {
          val cell:OtsRowCell = new OtsRowCell(column, Bytes.toBytes(row.getAs[String](column)))
          rec.addCell(cell)
        }
      }
      rec
    })

    hbaseContext.bulkPut[OtsRowRecord](recRdd, TableName.valueOf(getOtsTable(user.getTenantId, tableName)),
      (putOtsRecord) => {
        putOtsRecord.toPut
      })
  }

  /**
    * Get RDD[OtsRowRecord] with input [[config]] init
    *
    * @return RDD[OtsRowRecord]
    */
  def rdd: RDD[OtsRowRecord] = rdd(tableName, startKey, stopKey)

  private[ots] def rdd(tableName: String, startKey: String, stopKey: String): RDD[OtsRowRecord] = {
    val scan = new Scan()
    scan.setCaching(1000)
    scan.setStartRow(Bytes.fromHex(startKey))
    scan.setStopRow(Bytes.fromHex(stopKey))
    for ((_, v: SchemaQualifierDefinition) <- columns) {
      if (!v.columnFamily.isEmpty)
        scan.addColumn(Bytes.toBytes(v.columnFamily), Bytes.toBytes(v.qualifier))
    }

    hbaseContext.hbaseRDD(TableName.valueOf(getOtsTable(user.getTenantId, tableName)), scan).flatMapValues(r => {
      val recList = new java.util.ArrayList[OtsRowRecord]
      if (null != r) {
        recList.add(readResult(r))
      }
      JavaConversions.asScalaBuffer(recList)
    }).values
  }

  private[ots] def getOtsTable(tenantId: Long, tableName: String): String = tenantId + ":" + tableName

  /**
    * Reads the OTS_COLUMNS and converts it to a map of
    * SchemaQualifierDefinitions with the original sql column name as the key
    *
    * @param schemaMappingString The schema mapping string from the SparkSQL map
    * @return A map of definitions keyed by the SparkSQL column name
    */
  private def generateSchemaMappingMap(schemaMappingString: String):
  mutable.LinkedHashMap[String, SchemaQualifierDefinition] = {
    try {
      val resultingMap = new mutable.LinkedHashMap[String, SchemaQualifierDefinition]()

      resultingMap.put(OtsSparkConf.TABLE_COLUMN_HASHKEY,
        SchemaQualifierDefinition(OtsSparkConf.TABLE_COLUMN_HASHKEY,
          table.getHashkeyType match {
            case PrimaryKeyUtil.TYPE_NUMBER => "BIGINT"
            case PrimaryKeyUtil.TYPE_STRING => "STRING"
            case PrimaryKeyUtil.TYPE_BINARY => "STRING" //hex
          }, "", OtsSparkConf.TABLE_COLUMN_HASHKEY))

      if (table.getKeytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
        resultingMap.put(OtsSparkConf.TABLE_COLUMN_RANGEKEY,
          SchemaQualifierDefinition(OtsSparkConf.TABLE_COLUMN_RANGEKEY,
            table.getRangekeyType match {
              case PrimaryKeyUtil.TYPE_NUMBER => "BIGINT"
              case PrimaryKeyUtil.TYPE_STRING => "STRING"
              case PrimaryKeyUtil.TYPE_BINARY => "STRING" //hex
            }, "", OtsSparkConf.TABLE_COLUMN_RANGEKEY))
      }

      val columnDefinitions = schemaMappingString.split(',')
      columnDefinitions.foreach(cd => {
        val parts = cd.trim.split(':')

        //Make sure we get follow parts <columnName>:<columnType>
        if (parts.length == 2) {
          resultingMap.put(parts(0), SchemaQualifierDefinition(parts(0),
            parts(1), OtsConstants.DEFAULT_FAMILY_NAME, parts(0)))
        } else {
          throw new IllegalArgumentException("Invalid value for schema mapping '" + cd +
            "' should be '<columnName>:<columnType>' for columns")
        }
      })
      resultingMap
    } catch {
      case e: Exception => throw
        new IllegalArgumentException("Invalid value for " + OtsSparkConf.OTS_COLUMNS +
          " '" + schemaMappingString + "'", e)
    }
  }

  /**
    * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
    *
    * This method is used to keep ClassTags out of the external Java API, as
    * the Java compiler cannot produce them automatically. While this
    * ClassTag-faking does please the compiler, it can cause problems at runtime
    * if the Scala API relies on ClassTags for correctness.
    *
    * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior,
    * just worse performance or security issues.
    * For instance, an Array of AnyRef can hold any type T, but may lose primitive
    * specialization.
    */
  private[spark] def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}

/**
  * Construct to contains column data that spend SparkSQL and HBase
  *
  * @param columnName   SparkSQL column name
  * @param colType      SparkSQL column type
  * @param columnFamily HBase column family
  * @param qualifier    HBase qualifier name
  */
case class SchemaQualifierDefinition(columnName: String,
                                     colType: String,
                                     columnFamily: String,
                                     qualifier: String) extends Serializable {
  val columnFamilyBytes: Array[Byte] = Bytes.toBytes(columnFamily)
  val qualifierBytes: Array[Byte] = Bytes.toBytes(qualifier)
  val columnSparkSqlType: DataType =
    if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_BOOLEAN)) BooleanType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_INT)) IntegerType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_LONG)) LongType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_FLOAT)) FloatType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_DOUBLE)) DoubleType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_STRING)) StringType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_TIMESTAMP)) TimestampType
    else if (colType.equalsIgnoreCase(OtsSparkConf.OTS_COLUMN_TYPE_BINARY)) BinaryType //hex
    else throw new IllegalArgumentException("Unsupported column type :" + colType)
}
