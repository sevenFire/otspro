package com.baosight.xinsight.ots.client.table;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.TableException;


public class TableProvider {	
	/**
	 * 获取表列表
	 * 
	 */
	public List<String> getTablelistByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		List<String> lstNames = new ArrayList<String>();
		
		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			lstNames.add(name.getNameAsString());
		}		
		
		return lstNames;
	}
	
	/**
	 * 获取表的统计信息
	 *  
	 * @param tablename
	 * @return
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */	
	public Map<String, Long> getTableMetrics(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {	
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}

		//get regions for table
    	String fullTableName = getTableFullname(tenantid, tablename);
    	List<HRegionInfo> tableRegionInfos = admin.getTableRegions(TableName.valueOf(fullTableName));		
		Set<byte[]> tableRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		for (HRegionInfo regionInfo : tableRegionInfos) {
			tableRegions.add(regionInfo.getRegionName());
		}

	    ClusterStatus clusterStatus = admin.getClusterStatus();
	    Collection<ServerName> servers = clusterStatus.getServers();
	    final long megaByte = 1024L * 1024L;

	    long lReadRequestsCount = 0, lWriteRequestsCount = 0, lStorefileSize = 0, lRegionCount = 0;
	    lRegionCount = tableRegionInfos.size();
	    //iterate all cluster regions, filter regions from our table and compute their size
	    for (ServerName serverName: servers) {
	    	ServerLoad serverLoad = clusterStatus.getLoad(serverName);
	    	
	        for (RegionLoad regionLoad: serverLoad.getRegionsLoad().values()) {
	        	byte[] regionId = regionLoad.getName();
	        	
	        	if (tableRegions.contains(regionId)) {
	        		lReadRequestsCount += regionLoad.getReadRequestsCount();
	        		lWriteRequestsCount += regionLoad.getWriteRequestsCount();
	        		
	        		long regionSizeBytes = regionLoad.getStorefileSizeMB() * megaByte;
	        		lStorefileSize += regionSizeBytes;
	        	}
	        }
	    }		
	    
		Map<String, Long> resultMap = new HashMap<String, Long>();
		resultMap.put(OtsConstants.METRICS_READ_COUNT, lReadRequestsCount);
		resultMap.put(OtsConstants.METRICS_WRITE_COUNT, lWriteRequestsCount);
		resultMap.put(OtsConstants.METRICS_DISK_SIZE, lStorefileSize);
		resultMap.put(OtsConstants.METRICS_REGION_COUNT, lRegionCount);
		return resultMap;
	}
	
	
	/**
	 * 获取索引大小
	 * 
	 * @param tenantid
	 * @param tablename
	 * @return
	 */
	public long indexSizeTable(Admin admin, String tenantid, String tablename) {
		return 0;
	}
	
	
	/**
	 * 获取表属性信息
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */
	public boolean getTableEnableStatus(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException   {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + "is not exist!");
		}
		
    	String fullTableName = getTableFullname(tenantid, tablename);
    	return admin.isTableEnabled(TableName.valueOf(fullTableName));		
	}
	
	
	/**
	 * 获取表MaxVersions
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */
	public int getTableMaxVersions(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException   {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}		
		
    	String fullTableName = getTableFullname(tenantid, tablename);
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
		HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
	
		return columnDesc.getMaxVersions();
	}
	
	
	/**
	 * 获取表Compression Type
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */
	public String getTableCompressionType(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
			
    	String fullTableName = getTableFullname(tenantid, tablename);
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
		HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		
		return columnDesc.getCompressionType().getName();
	}
	
	
	/**
	 * 检查表是否存在
	 *
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 *
	 */
	public static boolean isTableExist(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    	String fullTableName = getTableFullname(tenantid, tablename);
    	return admin.tableExists(TableName.valueOf(fullTableName));
	}

	/**
	 * 检查表是否存在-wls
	 *
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 *
	 */
	public static boolean isTableExist(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		return admin.tableExists(TableName.valueOf("1:ots_" + tenantid));
	}


	public static boolean isTableExist(Admin admin, TableName tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {		
    	return admin.tableExists(tablename);
	}	
	
	/**
	 * 检查tenantid下所有表是否存在
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * 
	 */
	public static void createNamespaceIfnotExist(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {		
		NamespaceDescriptor descriptor = null;
		
		try {
			descriptor = admin.getNamespaceDescriptor(tenantid);
		} catch (NamespaceNotFoundException e) {
			if (descriptor == null) {
				admin.createNamespace(NamespaceDescriptor.create(tenantid).build());
			}
		}
	}
	

	/**
	 * 删除表
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * @throws InterruptedException 
	 * 
	 */
	public static void deleteTable(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException {
		if (!isTableExist(admin, tenantid, tablename)) {
			return;
		}		
		
    	String fullTableName = getTableFullname(tenantid, tablename);
    	//disable table before deleting it
    	if (admin.isTableEnabled(TableName.valueOf(fullTableName))) {
    		setTableEnableStatus(admin, TableName.valueOf(fullTableName), false);//hm 2016-12-15
		}
		admin.deleteTable(TableName.valueOf(fullTableName));
	}
	
	public static void deleteTable(Admin admin, TableName tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException {
		if (!isTableExist(admin, tableName)) {
			return;
		}		
		
    	//disable table before deleting it
    	if (admin.isTableEnabled(tableName)) {
    		setTableEnableStatus(admin, tableName, false);//hm 2016-12-15
		}
		admin.deleteTable(tableName);
	}
	

//	/**
//	 * 创建表
//	 *
//	 *
//	 * @throws IOException
//	 * @throws ZooKeeperConnectionException
//	 * @throws MasterNotRunningException
//	 * @throws TableException
//	 *
//	 */
//	public static void createTable(Admin admin, String tenantid, String tablename, String compressionType,
//			Integer maxVersions, Boolean mob_enabled, Integer mobThreshold, Boolean bReplication) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
//		if (tenantid == null || tenantid.isEmpty()) {
//			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is invalid!");
//		}
//		createNamespaceIfnotExist(admin, tenantid);
//
//		if (isTableExist(admin, tenantid, tablename)) {
//			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_EXIST, "table:" + tablename + " has exist!");
//		}
//
//    	String fullTableName = getTableFullname(tenantid, tablename);
//        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(fullTableName));
//        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
//		columnDesc.setScope(OtsConstants.DEFAULT_REPLICATED_SCOPE);
//		if (bReplication != null) {
//        	if (bReplication) {
//                columnDesc.setScope(OtsConstants.ENABLE_REPLICATED_SCOPE);
//			}
//		}
//
//        if (compressionType != null) {
//        	columnDesc.setCompressionType(convertCompression(compressionType));
//		}
//        else {
//        	columnDesc.setCompressionType(Algorithm.NONE);
//		}
//        if (maxVersions != null) {
//        	columnDesc.setMaxVersions(maxVersions.intValue());
//		}
//        if (mob_enabled != null){
//        	columnDesc.setMobEnabled(mob_enabled);
//        }
//        if (mobThreshold != null){
//        	columnDesc.setMobThreshold(mobThreshold*1024);//!!byte
//        }
//        tableDesc.addFamily(columnDesc);
//        //tableDesc.setMemStoreFlushSize(DefaultConstants.DEFAULT_MEM_STORE_FLUSH_SIZE);
//
//        admin.createTable(tableDesc);
//	}

	/**
	 * 创建表-大表
	 *
	 *
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 * @throws TableException
	 *
	 */
	public static void createTable(Admin admin, String tenantid,  String compressionType,
								   Integer maxVersions, Boolean mob_enabled, Integer mobThreshold, Boolean bReplication) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		if (tenantid == null || tenantid.isEmpty()) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace: 1(ots) is invalid!");
		}
		createNamespaceIfnotExist(admin, tenantid);

//		if (isTableExist(admin, tenantid, tablename)) {
		if (isTableExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_EXIST, "1:ots_" + tenantid + " has exist!");
		}

		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("1:ots_" + tenantid));
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		columnDesc.setScope(OtsConstants.DEFAULT_REPLICATED_SCOPE);
		if (bReplication != null) {
			if (bReplication) {
				columnDesc.setScope(OtsConstants.ENABLE_REPLICATED_SCOPE);
			}
		}

		if (compressionType != null) {
			columnDesc.setCompressionType(convertCompression(compressionType));
		}
		else {
			columnDesc.setCompressionType(Algorithm.NONE);
		}
		if (maxVersions != null) {
			columnDesc.setMaxVersions(maxVersions.intValue());
		}
		if (mob_enabled != null){
			columnDesc.setMobEnabled(mob_enabled);
		}
		if (mobThreshold != null){
			columnDesc.setMobThreshold(mobThreshold*1024);//!!byte
		}
		tableDesc.addFamily(columnDesc);
		//tableDesc.setMemStoreFlushSize(DefaultConstants.DEFAULT_MEM_STORE_FLUSH_SIZE);

		admin.createTable(tableDesc);
	}


	/**
	 * 转换名称
	 * 
	 * @param compressName
	 * @return
	 */
	private static Algorithm convertCompression(String compressName) {	
		if (compressName.equals(Algorithm.LZO.getName())) {
			return Algorithm.LZO;
		}
		else if (compressName.equals(Algorithm.GZ.getName())) {
			return Algorithm.GZ;
		}
		else if (compressName.equals(Algorithm.NONE.getName())) {
			return Algorithm.NONE;
		}
		else if (compressName.equals(Algorithm.SNAPPY.getName())) {
			return Algorithm.SNAPPY;
		}
		else if (compressName.equals(Algorithm.LZ4.getName())) {
			return Algorithm.LZ4;
		}
		else {
			throw new IllegalArgumentException("Unsupported compression algorithm name: " + compressName);
		}
	}	
	
	/**
	 * 更新表Versions
	 * 
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * @throws InterruptedException 
	 * @throws TableNotFoundException 
	 */
	public static void setTableVersions(Admin admin, String tenantid, String tablename, Integer iMaxVersions) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (iMaxVersions != null) {			
        	String fullTableName = getTableFullname(tenantid, tablename);
        	//disable table before modify it
        	if (admin.isTableEnabled(TableName.valueOf(fullTableName))) {
        		setTableEnableStatus(admin, TableName.valueOf(fullTableName), false);//hm 2016-12-15
    		}
        	HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));        
        	HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
        	columnDesc.setMaxVersions(iMaxVersions.intValue());
        	admin.modifyTable(TableName.valueOf(fullTableName), tableDesc);
    		setTableEnableStatus(admin, TableName.valueOf(fullTableName), true);//hm 2016-12-15
		}		
	}
	
	
	/**
	 * 更新表Truncate
	 * 
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * @throws InterruptedException 
	 */
	public static void setTableTruncate(Admin admin, String tenantid, String tablename, Boolean bTruncate) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException  {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (bTruncate != null) {
        	if (bTruncate.booleanValue()) {       		
            	String fullTableName = getTableFullname(tenantid, tablename);

            	//disable table before deleting it
            	if (admin.isTableEnabled(TableName.valueOf(fullTableName))) {
            		setTableEnableStatus(admin, TableName.valueOf(fullTableName), false);//hm 2016-12-15
        		}
        		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
        		
        		admin.deleteTable(TableName.valueOf(fullTableName));
        		admin.createTable(tableDesc);        		
			} 
        }        
	}
	
	
	/**
	 * 更新表Enable Status
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * @throws InterruptedException 
	 */
	public static void setTableEnableStatus(Admin admin, String tenantid, String tablename, Boolean bEnable) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (bEnable != null) {
        	String fullTableName = getTableFullname(tenantid, tablename);
        	boolean ret = admin.isTableEnabled(TableName.valueOf(fullTableName));	
        	if (bEnable.booleanValue() != ret) {			
				if (bEnable.booleanValue()) {
					//enableTable(admin, TableName.valueOf(fullTableName));
					admin.enableTable(TableName.valueOf(fullTableName));
				}
				else {
					//disableTable(admin, TableName.valueOf(fullTableName));
					admin.disableTable(TableName.valueOf(fullTableName));
				}	
        	}        	
        }		
	}
	
	public static void setTableEnableStatus(Admin admin, TableName tableName, Boolean bEnable) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, InterruptedException {
		if (!isTableExist(admin, tableName)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tableName + " is not exist!");
		}
		
		if (bEnable != null) {
        	boolean ret = admin.isTableEnabled(tableName);	
        	if (bEnable.booleanValue() != ret) {			
				if (bEnable.booleanValue()) {
					//enableTable(admin, tableName);
					admin.enableTable(tableName);
				}
				else {
					//disableTable(admin, tableName);
					admin.disableTable(tableName);
				}	
        	}        	
        }		
	}
	
	private static long getPauseTime(int tries, long pause) {
		int triesCount = tries;
		if (triesCount >= HConstants.RETRY_BACKOFF.length) {
			triesCount = HConstants.RETRY_BACKOFF.length - 1;
		}
		return pause * HConstants.RETRY_BACKOFF[triesCount];
	}

	public static void disableTable(Admin admin, TableName tableName) throws IOException {
		admin.disableTableAsync(tableName);
		// Wait until table is disabled
		boolean disabled = false;
		
		int numRetries = admin.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
		int retryLongerMultiplier = admin.getConfiguration().getInt("hbase.client.retries.longer.multiplier", 10);
		long pause = admin.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

		long start = System.currentTimeMillis();
		for (int tries = 0; tries < (numRetries * retryLongerMultiplier); tries++) {
			disabled = admin.isTableDisabled(tableName);
			if (disabled) {
				break;
			}
			long sleep = getPauseTime(tries, pause);

			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				// Do this conversion rather than let it out because do not want to change the method signature.
				throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
			}
		}
		if (!disabled) {
			long msec = System.currentTimeMillis() - start;
			throw new RegionException("Retries exhausted, it took too long to wait" + " for the table " + tableName + " to be disabled, after " + msec + "ms.");
		}
	}
	
	public static void findTable(Admin admin, TableName tableName) throws IOException {
		// Wait until table is found
		boolean found = false;
		
		int numRetries = admin.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
		int retryLongerMultiplier = admin.getConfiguration().getInt("hbase.client.retries.longer.multiplier", 10);
		long pause = admin.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

		long start = System.currentTimeMillis();
		for (int tries = 0; tries < (numRetries * retryLongerMultiplier); tries++) {
			found = admin.tableExists(tableName);
			if (found) {
				break;
			}
			long sleep = getPauseTime(tries, pause);

			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				// Do this conversion rather than let it out because do not want to change the method signature.
				throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
			}
		}
		if (!found) {
			long msec = System.currentTimeMillis() - start;
			throw new IOException("Table '" + tableName + "' not yet found, after " + msec + "ms.");
		}
	}
	
	public static void enableTable(Admin admin, TableName tableName) throws IOException {
		admin.enableTableAsync(tableName);

		// Wait until table is enabled
		boolean enabled = false;

		int numRetries = admin.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
		int retryLongerMultiplier = admin.getConfiguration().getInt("hbase.client.retries.longer.multiplier", 10);
		long pause = admin.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

		long start = System.currentTimeMillis();
		for (int tries = 0; tries < (numRetries * retryLongerMultiplier); tries++) {
			try {
				enabled = admin.isTableEnabled(tableName);
			} catch (TableNotFoundException tnfe) {
				// wait for table to be created
				enabled = false;
			}
			enabled = enabled && admin.isTableAvailable(tableName);
			if (enabled) {
				break;
			}
			long sleep = getPauseTime(tries, pause);

			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				// Do this conversion rather than let it out because do not want
				// to change the method signature.
				throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
			}
		}
		if (!enabled) {
			long msec = System.currentTimeMillis() - start;
			throw new IOException("Table '" + tableName + "' not yet enabled, after " + msec + "ms.");
		}
	}
	
	/**
	 * 设置列族是否启用mob
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */
	public static void enableMob(Admin admin, String tenantid, String tablename, Boolean bEnable) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (bEnable != null) {
        	String fullTableName = getTableFullname(tenantid, tablename);
        	HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
        	HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
        	columnDesc.setMobEnabled(bEnable.booleanValue());
        	admin.modifyColumn(TableName.valueOf(fullTableName), columnDesc);
        }		
	}
	
	/**
	 * 设置mob threshold
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */
	public static void setMobThreshold(Admin admin, String tenantid, String tablename, Integer mobThreshold) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (mobThreshold != null)
		{
			String fullTableName = getTableFullname(tenantid, tablename);
			HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
	    	HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
	    	columnDesc.setMobThreshold(mobThreshold.longValue()*1024);//!!byte
        	admin.modifyColumn(TableName.valueOf(fullTableName), columnDesc);
		}        
	}
	
	/**
	 * 设置replication scope
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */
	public static void setReplicationScope(Admin admin, String tenantid, String tablename, Boolean bReplication) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		if (!isTableExist(admin, tenantid, tablename)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table:" + tablename + " is not exist!");
		}
		
		if (bReplication != null)
		{
			String fullTableName = getTableFullname(tenantid, tablename);
			HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(fullTableName));
	    	HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
	    	if (bReplication) {
		    	columnDesc.setScope(OtsConstants.ENABLE_REPLICATED_SCOPE);
			} else {
		    	columnDesc.setScope(OtsConstants.DEFAULT_REPLICATED_SCOPE);
			}
	    	admin.modifyColumn(TableName.valueOf(fullTableName), columnDesc);
		}        
	}
	
	/**
	 * 获取表全名
	 * 
	 * @param tenantid
	 * @param tablename
	 * @return
	 */
	public static String getTableFullname(String tenantid, String tablename) {
		return (tenantid + TableName.NAMESPACE_DELIM + tablename);
	}
	
	
	/**
	 * 获取表名
	 * 
	 * @param fullTableName
	 * @return
	 */
	public String getTablename(String fullTableName) {
		String[] values = fullTableName.split(String.valueOf(TableName.NAMESPACE_DELIM));
		return values[1];
	}
}
