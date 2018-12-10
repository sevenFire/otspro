package com.baosight.xinsight.ots.spark

import java.io.Serializable
import java.sql.{Connection, DriverManager, SQLException}

import com.baosight.xinsight.ots.{OtsConstants, OtsErrorCode}
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil
import com.baosight.xinsight.ots.exception.OtsException
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

class MetaHelper(private val server: String, //ip:port
                 private val username: String,
                 private val password: String,
                 private val database: String = "ots") extends Serializable with Logging {

  private lazy val url: String = String.format("jdbc:postgresql://%s/%s", server, database)

  private val TIMEOUT: Int = 3 //second
  private val driver = "org.postgresql.Driver"

  @transient
  private var connection: Connection = {
    Class.forName(driver)
    null
  }

  @throws[OtsException]
  def getTableByName(tenantId: Long, tableName: String): OtsTable = {
    import java.sql.SQLException

    import com.baosight.xinsight.ots.OtsErrorCode

    var table: OtsTable = null
    try {
      connect()
      val st = connection.createStatement
      //val sql = String.format("select * from ots_user_table where ots_user_table.name = '%s' and ots_user_table.tid = '%d';", tableName, tenantId)
      val sql = new StringBuffer
      sql.append("select * from ots_user_table where ots_user_table.name = '")
      sql.append(tableName)
      sql.append("'and ots_user_table.tid = '")
      sql.append(tenantId)
      sql.append("';")
      logDebug(sql.toString)

      val rs = st.executeQuery(sql.toString)
      if (rs.next) {
        table = new OtsTable
        table.setUid(rs.getLong("uid"))
        table.setName(tableName)
        table.setId(rs.getLong("id"))
        table.setDesp(rs.getString("desc"))
        table.setCompression(rs.getString("compression"))
        table.setEnable(rs.getShort("enable"))
        table.setMaxversion(rs.getInt("maxversion"))
        table.setTid(tenantId)
        table.setMobEnabled(rs.getShort("mob_enabled"))
        table.setMobThreshold(rs.getLong("mob_threshold"))
        table.setKeytype(rs.getInt("keytype"))
        table.setHashkeyType(rs.getInt("hashkey_type"))
        table.setRangekeyType(rs.getInt("rangekey_type"))
      }
      st.close()
    } catch {
      case e: SQLException =>
        throw new OtsException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query table " + tableName + "!\n" + e.getMessage)
    } finally {
      disConnect()
    }
    table
  }

  @throws[OtsException]
  def connect(): Unit = {
    try {
      if (null == connection || connection.isClosed)
        connection = DriverManager.getConnection(url, username, password)
      if (!connection.isValid(TIMEOUT)) {
        connection.close()
        connection = DriverManager.getConnection(url, username, password)
      }
    } catch {
      case e: SQLException =>
        throw new OtsException(OtsErrorCode.EC_RDS_FAILED_CONN, "Failed to connect to database!\n" + ExceptionUtils.getFullStackTrace(e))
    }
  }

  @throws[OtsException]
  def disConnect(): Unit = {
    try {
      if (connection != null) connection.close()
    }
    catch {
      case e: SQLException =>
        throw new OtsException(OtsErrorCode.EC_RDS_FAILED_CLOSE_CONN, "Failed to disconnect to database!\n" + ExceptionUtils.getFullStackTrace(e))
    }
  }
}

class OtsTable extends Serializable {
  private var id = 0L
  private var uid = 0L
  private var name: String = _
  private var compression: String = _
  private var desc: String = _
  private var enable = 0
  private var maxversion = 0L
  private var tid = 0L
  private var mobThreshold = 0L
  private var mobEnabled = 0
  private var keytype = 0
  private var hashkeyType = 0
  private var rangekeyType = 0

  def getKeytype: Int = keytype

  def setKeytype(keytype: Int): Unit = {
    this.keytype = keytype
  }

  def getHashkeyType: Int = hashkeyType

  def setHashkeyType(hashkeyType: Int): Unit = {
    this.hashkeyType = hashkeyType
  }

  def getRangekeyType: Int = rangekeyType

  def setRangekeyType(rangekeyType: Int): Unit = {
    this.rangekeyType = rangekeyType
  }

  def getMobThreshold: Long = mobThreshold

  def setMobThreshold(mobThreshold: Long): Unit = {
    this.mobThreshold = mobThreshold
  }

  def getMobEnabled: Int = mobEnabled

  def setMobEnabled(mobEnabled: Int): Unit = {
    this.mobEnabled = mobEnabled
  }

  def getUid: Long = uid

  def setUid(uid: Long): Unit = {
    this.uid = uid
  }

  def getName: String = name

  def setName(name: String): Unit = {
    this.name = if (name == null) null
    else name.trim
  }

  def getCompression: String = compression

  def setCompression(compression: String): Unit = {
    this.compression = if (compression == null) null
    else compression.trim
  }

  def getDesp: String = desc

  def setDesp(desc: String): Unit = {
    this.desc = if (desc == null) null
    else desc.trim
  }

  def getId: Long = id

  def setId(id: Long): Unit = {
    this.id = id
  }

  def getEnable: Int = enable

  def setEnable(enable: Int): Unit = {
    this.enable = enable
  }

  def getMaxversion: Long = maxversion

  def setMaxversion(maxversion: Long): Unit = {
    this.maxversion = maxversion
  }

  def getTid: Long = tid

  def setTid(tid: Long): Unit = {
    this.tid = tid
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(" [")
    sb.append("Hash = ").append(hashCode)
    sb.append(", id=").append(id)
    sb.append(", name=").append(name)
    sb.append(", keytype=").append(keytype)
    sb.append(", compression=").append(compression)
    sb.append(", desc=").append(desc)
    sb.append(", enable=").append(enable)
    sb.append(", maxversion=").append(maxversion)
    sb.append(", tenantId=").append(tid)
    sb.append(", userId=").append(uid)
    sb.append(", hashkeyType=").append(hashkeyType)
    sb.append(", rangekeyType=").append(rangekeyType)
    sb.append(", mobThreshold=").append(mobThreshold)
    sb.append(", mobEnabled=").append(mobEnabled)
    sb.append("]")
    sb.toString
  }
}

class OtsRowRecord extends Serializable {
  private var rowkey: Array[Byte] = _
  private var cellMap = new scala.collection.mutable.LinkedHashMap[String, OtsRowCell]

  def this(cellMap: scala.collection.mutable.LinkedHashMap[String, OtsRowCell]) {
    this()
    this.cellMap = cellMap
  }

  def this(rec: Result) {
    this()
    this.rowkey = rec.getRow

    rec.rawCells.foreach(cell => {
      if (cell != null) {
        val qualifier: String = Bytes.toString(CellUtil.cloneQualifier(cell))
        val cellRow: OtsRowCell = new OtsRowCell(qualifier, cell.getTimestamp, CellUtil.cloneValue(cell))
        this.addCell(cellRow)
      }
    })
  }

  def toPut: Put = {
    if (this.getRowkey == null)
      throw new IllegalArgumentException("invalid ots record, lack of row key")

    if (this.getHashkey == null)
      throw new IllegalArgumentException("invalid ots record, lack of hash key")

    val put = new Put(this.rowkey)

    for (cell <- this.cellMap) {
      if (cell != null)
        put.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), Bytes.toBytes(cell._1), cell._2.getValue)
    }
    put
  }

  def addCell(c: OtsRowCell): Unit = {
    if (c != null) this.cellMap.put(c.getQualifier, c)
  }

  def getCellMap: scala.collection.mutable.LinkedHashMap[String, OtsRowCell] = this.cellMap

  def setCellMap(cellMap: scala.collection.mutable.LinkedHashMap[String, OtsRowCell]): Unit = {
    this.cellMap = cellMap
  }

  def getCell(qualifier: String): OtsRowCell = {
    this.cellMap(qualifier)
  }

  def clear(): Unit = {
    this.cellMap.clear()
  }

  def getRowkey: Array[Byte] = this.rowkey

  def getHashkey: Array[Byte] = PrimaryKeyUtil.getHashKey(this.rowkey)

  /**
    * if table's key type is only hash style, return empty Array[byte]
    */
  def getRangekey: Array[Byte] = PrimaryKeyUtil.getRangeKey(this.rowkey)

  def setRowkey(rowkey: Array[Byte]): Unit = {
    this.rowkey = rowkey
  }

  def hasRowkey: Boolean = this.rowkey != null

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(" [")
    sb.append("Hash = ").append(hashCode)
    sb.append(", rowkey=").append(Bytes.toHex(rowkey))
    sb.append(", cellMap=").append(cellMap)
    sb.append("]")
    sb.toString
  }
}

class OtsRowCell extends Serializable {
  private var qualifier: String = _
  private var value: Array[Byte] = _
  private var timestamp: Long = 0L

  def this(qualifier: String, timestamp: Long, value: Array[Byte]) {
    this()
    this.qualifier = qualifier
    this.timestamp = timestamp
    this.value = value
  }

  def this(name: String, value: Array[Byte]) {
    this(name, null.asInstanceOf[Long], value)
  }

  def getQualifier: String = this.qualifier

  def setQualifier(qualifier: String): Unit = {
    this.qualifier = qualifier
  }

  def getValue: Array[Byte] = this.value

  def setValue(value: Array[Byte]): Unit = {
    this.value = value
  }

  def getTimestamp: Long = this.timestamp

  /**
    * only for query
    */
  def setTimestamp(timestamp: Long): Unit = {
    this.timestamp = timestamp
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(" [")
    sb.append("Hash = ").append(hashCode)
    sb.append(", qualifier=").append(qualifier)
    sb.append(", value=").append(Bytes.toHex(value))
    sb.append(", timestamp=").append(timestamp)
    sb.append("]")
    sb.toString
  }
}