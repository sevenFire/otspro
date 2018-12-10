package com.baosight.xinsight.ots.client.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.common.table.TableMetrics;

public class MetricsProvider {
	/**
	 * 获取tenantid下表列表
	 * 
	 */
	public List<String> getTablelistByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		List<String> lstNames = new ArrayList<String>();
		
		String fullTableNamePrefix = tenantid + TableName.NAMESPACE_DELIM;
		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			lstNames.add(name.getNameAsString().substring(fullTableNamePrefix.length()));
		}
		
		return lstNames;
	}
		
	/**
	 * 获取tenantid下表的统计信息
	 *  
	 * @param tablename
	 * @return
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */	
	public static Map<String, TableMetrics> getTableMetricsByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {	
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}
		
	    ClusterStatus clusterStatus = admin.getClusterStatus();
	    Collection<ServerName> servers = clusterStatus.getServers();
	    final long megaByte = 1024L * 1024L;
	    
	    Map<String, TableMetrics> resultMap = new HashMap<String, TableMetrics>();
		String fullTableNamePrefix = tenantid + TableName.NAMESPACE_DELIM;

		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			String tablename = name.getNameAsString();
			//get regions for table
			List<HRegionInfo> tableRegionInfos = admin.getTableRegions(TableName.valueOf(tablename));
			Set<byte[]> tableRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
			for (HRegionInfo regionInfo : tableRegionInfos) {
				tableRegions.add(regionInfo.getRegionName());
			}
				
		    long lReadRequestsCount = 0, lWriteRequestsCount = 0, lStorefileSize = 0, lRegionCount = 0;
		    lRegionCount =  tableRegionInfos.size();
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
		    
		    TableMetrics metrics = new TableMetrics(tablename.substring(fullTableNamePrefix.length()));
		    metrics.setlReadRequestsCount(lReadRequestsCount);
		    metrics.setlStorefileSize(lStorefileSize);
		    metrics.setlWriteRequestsCount(lWriteRequestsCount);
		    metrics.setlRegionCount(lRegionCount);
		    resultMap.put(tablename.substring(fullTableNamePrefix.length()), metrics);
		}	    
		return resultMap;
	}
	
	
	public static TableMetrics getTableMetrics(Admin admin, String tenantid, String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {	
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}
		
	    ClusterStatus clusterStatus = admin.getClusterStatus();
	    Collection<ServerName> servers = clusterStatus.getServers();
	    final long megaByte = 1024L * 1024L;	    
	
		String fullTableName = tenantid + TableName.NAMESPACE_DELIM + tablename;
		//get regions for table
		List<HRegionInfo> tableRegionInfos = admin.getTableRegions(TableName.valueOf(fullTableName));
		Set<byte[]> tableRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		for (HRegionInfo regionInfo : tableRegionInfos) {
			tableRegions.add(regionInfo.getRegionName());
		}
			
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
	    
	    TableMetrics metrics = new TableMetrics(tablename);
	    metrics.setlReadRequestsCount(lReadRequestsCount);
	    metrics.setlStorefileSize(lStorefileSize);
	    metrics.setlWriteRequestsCount(lWriteRequestsCount);	
	    metrics.setlRegionCount(lRegionCount);
		return metrics;
	}	
	
	/**
	 * 获取命名空间的索引大小
	 * 
	 * @param tenantid
	 * @return
	 */
	public long indexSizeTablebyNamespace(Admin admin, String tenantid) {
		return 0;

	}
	
	
	/**
	 * 获取tenantid下表属性信息
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */
	public Map<String, Boolean> getTableEnableStatusByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException   {
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}
		
		Map<String, Boolean> resultMap = new HashMap<String, Boolean>();
		String fullTableNamePrefix = tenantid + TableName.NAMESPACE_DELIM;

		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			String tablename = name.getNameAsString();
			boolean ret = admin.isTableEnabled(TableName.valueOf(tablename));
			resultMap.put(tablename.substring(fullTableNamePrefix.length()), ret);
		}			
		
		return resultMap;
	}
	
	
	/**
	 * 获取tenantid下所有表MaxVersions
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */	
	public Map<String, Integer> getTableMaxVersionsByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException   {
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}		
		
		Map<String, Integer> resultMap = new HashMap<String, Integer>();
		String fullTableNamePrefix = tenantid + TableName.NAMESPACE_DELIM;
		
		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			String tablename = name.getNameAsString();
			HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tablename));
			HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
			
			resultMap.put(tablename.substring(fullTableNamePrefix.length()), columnDesc.getMaxVersions());
		}
		
		return resultMap;
	}
	
	
	/**
	 * 获取tenantid下所有表Compression Type
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */	
	public Map<String, String> getTableCompressionTypeByNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}
		
		Map<String, String> resultMap = new HashMap<String, String>();
		String fullTableNamePrefix = tenantid + TableName.NAMESPACE_DELIM;
		
		TableName[] tableNames = admin.listTableNamesByNamespace(tenantid);
		for (TableName name: tableNames) {
			String tablename = name.getNameAsString();
			HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tablename));
			HColumnDescriptor columnDesc = tableDesc.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));			
			
			resultMap.put(tablename.substring(fullTableNamePrefix.length()), columnDesc.getCompressionType().getName());
		}
		
		return resultMap;
	}
	
		
	/**
	 * 检查tenantid下所有表是否存在
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * 
	 */	
	public static boolean isTableNamespaceExist(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {		
		NamespaceDescriptor descriptor = null;
		try {
			descriptor = admin.getNamespaceDescriptor(tenantid);
		} catch (NamespaceNotFoundException e) {
			//e.printStackTrace();
			//System.out.println("namespace:" + tenantid + " is not exist!");
		}
		
		return descriptor==null?false:true;
	}
	
	
	/**
	 * 删除tenantid下所有表
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */
	public void deleteTableNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
		if (!isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}	
		
		admin.deleteNamespace(tenantid);
	}
	
	
	/**
	 * 创建表tenantid
	 * 
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 * 
	 */	
	public void createTableNamespace(Admin admin, String tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		if (isTableNamespaceExist(admin, tenantid)) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "namespace:" + tenantid + " is not exist!");
		}
		
		admin.createNamespace(NamespaceDescriptor.create(tenantid).build());	
	}
	
	
	public static void main(String[] args) throws IOException {
		Configuration hbase_cfg = HBaseConfiguration.create();
    	hbase_cfg.setStrings(HConstants.ZOOKEEPER_QUORUM, "168.2.4.58,168.2.4.59,168.2.4.60");
    	@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(hbase_cfg);	    

		String tablename = "huang:ming200w";
		//get regions for table
		List<HRegionInfo> tableRegionInfos = admin.getTableRegions(TableName.valueOf(tablename));
		for (HRegionInfo regionInfo : tableRegionInfos) {
			String name = regionInfo.getRegionNameAsString();
			byte[] start_key = regionInfo.getStartKey();
			byte[] end_key = regionInfo.getEndKey();
			
			System.out.println(name + ":" + Bytes.toString(start_key) + " to " + Bytes.toString(end_key));
		}    
		admin.close();
	}
}
