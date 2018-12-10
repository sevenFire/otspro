package com.baosight.xinsight.ots.client;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.log4j.Logger;

import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.client.exception.SecondaryIndexException;
import com.baosight.xinsight.ots.client.index.IndexConfigurator;
import com.baosight.xinsight.ots.client.metacfg.Configurator;
import com.baosight.xinsight.ots.client.metacfg.Index;
import com.baosight.xinsight.ots.client.metacfg.Table;
import com.baosight.xinsight.ots.client.table.MetricsProvider;
import com.baosight.xinsight.ots.client.table.RecordProvider;
import com.baosight.xinsight.ots.client.table.RecordQueryOption;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.table.RowRecord;
import com.baosight.xinsight.ots.client.table.TableProvider;
import com.baosight.xinsight.ots.client.util.ConnectionUtil;
import com.baosight.xinsight.ots.common.util.SecondaryIndexUtil;
import com.baosight.xinsight.ots.common.index.ColumnListConvert;
import com.baosight.xinsight.ots.common.index.IndexInfo;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumnListConvert;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.yarn.YarnAppUtil;
import com.baosight.xinsight.yarn.YarnTagGenerator;

/**
 * OtsTable
 * 
 * @author huangming
 * @created 2015.08.06
 */
public class OtsTable {
	private static final Logger LOG = Logger.getLogger(OtsTable.class);

	private Long userid;
	private Long tenantid;
	private String tablename;
	private Table info = null;
	private OtsConfiguration conf = null;
	
	public OtsTable(Table info, Long tenantid, OtsConfiguration conf) {
		this.info = info;
		this.tenantid = tenantid;
		this.conf = conf;
		if (info != null) {
			this.userid = info.getUid();
			this.tablename = info.getName();
		}
	}
	
	public OtsTable(Long userid, Long tenantid, String tablename, OtsConfiguration conf) {
		this.userid = userid;
		this.tenantid = tenantid;
		this.tablename = tablename;
		this.conf = conf;
	}

	public Long getUserid() {
		return userid;
	}
	
	public Table getInfo() throws ConfigException {
		if (this.info != null) {
			return this.info;
		} else { //no safe mode
			Configurator configurator = new Configurator();

			try {
				return configurator.queryTable(getTenantid(), getName());				
			} catch (ConfigException e) {
				e.printStackTrace();
				throw e;
			} finally {
				configurator.release();
			}
		}
	}
	
	public long getId() throws IOException, ConfigException {
		return getInfo().getId();
	}
	
	public String getName() {
		return this.tablename;
	}

	public long getTenantid() {
		return this.tenantid;
	}
	
	public String getTenantidAsString() throws IOException, ConfigException {
		return String.valueOf(getTenantid());
	}
	
	public String getCompression() throws IOException, ConfigException {
		return getInfo().getCompression();
	}

	public String getDescription() throws IOException, ConfigException {
		return getInfo().getDesp();
	}

	public boolean getEnable() throws IOException, ConfigException {
		return (getInfo().getEnable() == 1?true:false);
	}

	public long getMaxversion() throws IOException, ConfigException {
		return getInfo().getMaxversion();
	}

	public long getMobThreshold() throws IOException, ConfigException {
		return getInfo().getMobThreshold();
	}

	public boolean getMobEnabled() throws IOException, ConfigException {
		return (getInfo().getMobEnabled() == 1?true:false);
	}
	
	public int getKeyType() throws ConfigException {
		return getInfo().getKeytype();
	}
	
	public int getHashKeyType() throws ConfigException {
		return getInfo().getHashkeyType();
	}
		
	public int getRangeKeyType() throws ConfigException {
		return getInfo().getRangekeyType();
	}
	
	public Date getCreateTime() throws ConfigException {
		return getInfo().getCreateTime();
	}
	
	public Date getModifyTime() throws ConfigException {
		return getInfo().getModifyTime();
	}
	
	public long getModifyUserid() throws ConfigException {
		return getInfo().getModifyUid();
	}
	
	public void updateTable(String description, Integer maxVersions, Boolean bEnabled, Boolean bMobEnabled, Integer mobThreshold, Boolean replicationEnable) throws ConfigException, OtsException, TableException {
		Admin admin = null;
    	try {
    		admin = ConnectionUtil.getInstance().getAdmin();
    		//if (maxVersions != null) {
        	//	TableProvider.setTableVersions(admin, getTenantidAsString(), getName(), maxVersions);
			//}
    		//if (bEnabled != null) {
    		//	TableProvider.setTableEnableStatus(admin, getTenantidAsString(), getName(), bEnabled);
			//}
    		if (bMobEnabled != null) {
    			TableProvider.enableMob(admin, getTenantidAsString(), getName(), bMobEnabled);
			}
    		if (mobThreshold != null) {
    			TableProvider.setMobThreshold(admin, getTenantidAsString(), getName(), mobThreshold);
			}
    		if (replicationEnable != null) {
    			TableProvider.setReplicationScope(admin, getTenantidAsString(), getName(), replicationEnable);
			}
		}  catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to update table info for hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to update table info for hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to update table info for can not connect to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to update table info for can not connect to zookeeper!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to update table info !\n" + e.getMessage());			
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE, "Failed to update table info !\n" + e.getMessage());			
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        
        ///////////
		boolean updateRdb = false;		
		Configurator configurator = new Configurator();
		try {
			
			Table table = configurator.queryTable(getTenantid(), getName());
			if (table == null) {
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, String.format("user (id:%d) was not owned table:%s!", getUserid(), getName()));
			}	
			if (description != null){
				updateRdb = true;
				table.setDesp(description);
			}
			//if(bEnabled != null){
			//	updateRdb = true;
			//	table.setEnable(bEnabled==true?1:0);
			//}
			//if (maxVersions != null) {
			//	updateRdb = true;
			//	table.setMaxversion(maxVersions);
			//}
			if (bMobEnabled != null) {
				updateRdb = true;
				table.setMobEnabled(bMobEnabled==true?1:0);
			}
			if (mobThreshold != null) {
				updateRdb = true;
				table.setMobThreshold(mobThreshold);
			}
							
			if (updateRdb) {
				table.setModifyTime(new Date());
				table.setModifyUid(getUserid());
				configurator.updateTable(table);
				this.info = table;
			}				
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} catch (OtsException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to update table info !\n" + e.getMessage());			
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE, "Failed to update table info !\n" + e.getMessage());
		} finally {
			configurator.release();
        }
	}	

	public void truncate() throws Exception {

		Admin admin = null;
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
        	TableProvider.setTableTruncate(admin, getTenantidAsString(), getName(), true);
		}  catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to update table info for hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to update table info for hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to update table info for can not connect to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to update table info for can not connect to zookeeper!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to update table info !\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE, "Failed to update table info !\n" + e.getMessage());
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

        ///////////
		Configurator configurator = new Configurator();
		try {

			Table table = configurator.queryTable(getTenantid(), getName());
			if (table == null) {
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, String.format("user (id:%d) was not owned table:%s!", getUserid(), getName()));
			}

			table.setModifyTime(new Date());
			table.setModifyUid(getUserid());
			configurator.updateTable(table);
			this.info = table;

		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} catch (OtsException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to update table info !\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE, "Failed to update table info !\n" + e.getMessage());
		} finally {
			configurator.release();
        }
	}



	public void enable(boolean enabled) throws Exception {
		updateTable(null, null, enabled, null, null, null);
	}

	public void modifyDescription(String description) throws Exception {
		updateTable(description, null, null, null, null, null);
	}

	public void mobEnable(boolean enabled) throws Exception {
		updateTable(null, null, null, enabled, null, null);
	}
	
	public void modifyMobThreshold(Integer mobThreshold) throws Exception {
		updateTable(null, null, null, getMobEnabled(), mobThreshold, null);
	}
	
	public void replicationEnable(boolean enabled) throws Exception {
		updateTable(null, null, null, null, null, enabled);
	}	
	
	////////////////////////
	public void createIndex(IndexInfo model) throws ConfigException, IndexException, IOException, OtsException {
		Configurator configurator = new Configurator();
		try {			
			if(model.getColumns().size() == 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NUM, "lack index column");
			}
			
			//check duplicate columns and empty name
			if (model.checkColumnsDuplicateAndEmpty()) {
				LOG.warn("create index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_DUPLICATE_COLUMN, "create index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
			}
			
			if(model.getShardNum() <= 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_SHARE_NUM, "Invalid shard num:" + model.getShardNum());
			}
			if(model.getReplicationNum() <= 0 ) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_REPLICATION, "Invalid replication:" + model.getReplicationNum());
			}
			if(model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_ONLINE && 
					model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_OFFLINE) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_PATTERN, "Invalid Pattern:" + model.getPattern());
			}
			
			// add index in rdb
			Index index = new Index();
			index.setName(model.getName());
			index.setShardNum(model.getShardNum());
			index.setReplicationNum(model.getReplicationNum());
			index.setStartKey(model.getStartKey());
			index.setEndKey(model.getEndKey());
			index.setTid(getId());
			index.setUid(getUserid());
			index.setCreateTime(new Date());
			index.setLastModify(new Date());			
			ColumnListConvert convert = new ColumnListConvert();
			convert.setColumnList(model.getColumns());
			index.setIndexColumns(convert.toString());			
			index.setPattern(model.getPattern());
			configurator.addIndex(index);
			
			IndexConfigurator indexConfigurator = new IndexConfigurator();
			try {	
				indexConfigurator.CreateIndex(getTenantidAsString(), getName(), model.getName(), model);
			} catch (IndexException e) {
				e.printStackTrace();
				LOG.warn("Create index "+ getName() + "." + model.getName() + " in solr failed!");
				
				configurator.delIndex(getId(), model.getName());				
				throw e;
			}

			try {	
				indexConfigurator.BuildIndex(getTenantidAsString(), getName(), model.getName());
			} catch (IndexException e) {
				e.printStackTrace();
				LOG.warn("Build index "+ getName() + "." + model.getName() + " in solr failed!");
				
				configurator.delIndex(getId(), model.getName());				
				throw e;
			}			
		} catch (IndexException e) {
			e.printStackTrace();						
			throw e;
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} finally {
			configurator.release();
		}		
	}
	
	public void innerCreateSolrIndex(IndexInfo model) throws ConfigException, IndexException, IOException, OtsException {
		try {			
			if(model.getColumns().size() == 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NUM, "lack index column");
			}
			
			//check duplicate columns and empty name
			if (model.checkColumnsDuplicateAndEmpty()) {
				LOG.warn("create index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_DUPLICATE_COLUMN, "create index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
			}
			
			if(model.getShardNum() <= 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_SHARE_NUM, "Invalid shard num:" + model.getShardNum());
			}
			if(model.getReplicationNum() <= 0 ) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_REPLICATION, "Invalid replication:" + model.getReplicationNum());
			}
			if(model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_ONLINE && 
					model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_OFFLINE) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_PATTERN, "Invalid Pattern:" + model.getPattern());
			}
						
			IndexConfigurator indexConfigurator = new IndexConfigurator();
			try {	
				indexConfigurator.CreateIndex(getTenantidAsString(), getName(), model.getName(), model);
			} catch (IndexException e) {
				e.printStackTrace();
				LOG.warn("Create index "+ getName() + "." + model.getName() + " in solr failed!");
				
				throw e;
			}

			try {	
				indexConfigurator.BuildIndex(getTenantidAsString(), getName(), model.getName());
			} catch (IndexException e) {
				e.printStackTrace();
				LOG.warn("Build index "+ getName() + "." + model.getName() + " in solr failed!");
				
				throw e;
			}			
		} catch (IndexException e) {
			e.printStackTrace();						
			throw e;
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} finally {
			
		}		
	}

	/**
	 * delete index by name
	 * 
	 * @param indexname
	 * @throws ConfigException 
	 * @throws IOException 
	 * @throws IndexException 
	 */
	public void deleteIndex(String indexname) throws OtsException, IndexException, IOException, ConfigException {		
		LOG.info("Delete index " + getName() + "." + indexname);
		
		IndexConfigurator indexConfigurator = new IndexConfigurator();
		indexConfigurator.DeleteIndex(getTenantidAsString(), getName(), indexname);
		
		Configurator configurator = new Configurator();
		try {
			Index index = configurator.queryIndex(getTenantid(), getName(), indexname);
			if(null != index) {
				configurator.delIndexProfile(index.getTid());
				configurator.delIndex(index.getTid(), indexname);
			} else {
				LOG.error("Delete index failed! " + getName() + "." + indexname +" no exist!");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, "Delete index failed! " + getName() + "." + indexname +" no exist!");
			}
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} catch (OtsException e) {
			e.printStackTrace();
			throw e;
		} finally {
			configurator.release();
		}	
	}

	public OtsIndex getIndex(String indexname) throws ConfigException, IOException {
		Configurator configurator = new Configurator();

		try {
			Index index = configurator.queryIndex(getTenantid(), getName(), indexname);
			if (index != null) {
				return new OtsIndex(getTenantid(), getUserid(), getName(), index, this.conf);
			}			
			return null;
			
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} finally {
			configurator.release();
		}
	}
	
	public OtsIndex getIndexNoSafe(String indexname) throws Exception{
		return new OtsIndex(getTenantid(), getUserid(), getName(), indexname, this.conf);
	}
	
	public List<OtsIndex> getAllIndexes() throws ConfigException, IOException {
		List<OtsIndex> lstIndex = new ArrayList<OtsIndex>();
		
		Configurator configurator = new Configurator();
		try {
			List<Index> lstIndexes = configurator.queryTableIndex(getTenantid(), getName());
			for (Index index: lstIndexes) {				
				lstIndex.add(new OtsIndex(getTenantid(), getUserid(), getName(), index, this.conf));
			}				
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} finally {
			configurator.release();
        }
		
		return lstIndex;
	}
	
	public List<SecondaryIndexInfo> getAllSecondaryIndexeInfos() throws IOException, OtsException {
		Admin admin = null;		
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			String tableFullName = getTableFullname(getTenantid(), getName());
		    TableName userTable = TableName.valueOf(tableFullName);
	    	HTableDescriptor userTableDescriptor = admin.getTableDescriptor(userTable);
	    	return SecondaryIndexUtil.parseIndexes(userTableDescriptor.getValue(OtsConstants.OTS_INDEXES));
		} finally {
 			try {
 				if (admin != null) {
 					admin.close();
 				}
 			} catch (IOException e) {
 				e.printStackTrace();
 			}
 		}
	}

	////////////////////////////////////record

	public void insertRecord(RowRecord record) throws OtsException, TableException {
		List<RowRecord> records = new ArrayList<RowRecord>();
		records.add(record);
		insertRecords(records);
	}

	public void insertRecords(List<RowRecord> records) throws OtsException, TableException {
		
		try {

			RecordProvider.insertRecords(
					ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())),
					getId(),
					records);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to put records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to insert records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to put records because can not connect to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to insert records because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to put records!" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_INSERT, "Failed insert put records!\n" + e.getMessage());
		} 
	}

	public void updateRecord(RowRecord record) throws Exception {
		List<RowRecord> records = new ArrayList<RowRecord>();
		records.add(record);
		updateRecords(records);
	}

	public void updateRecords(List<RowRecord> records) throws Exception {
		
		try {
        	RecordProvider.insertRecords(
        			ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())),
					getId(), records);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to put records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to put records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to put records because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to put records because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to put records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_INSERT, "Failed insert put records!\n" + e.getMessage());
		} 		
	}
	
	private String getTableFullname(long tenantid, String tablename) {
		return ("1" + TableName.NAMESPACE_DELIM + "ots_" + String.valueOf(tenantid));
	}

	public void deleteRecord(byte[] key) throws Exception {
		List<byte[]> keys = new ArrayList<byte[]>();
		keys.add(key);
		deleteRecords(keys);
	}
	
	public void deleteRecords(List<byte[]> keys) throws Exception {
		
		try {	
			RecordProvider.deleteRecordsByKeys(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), keys);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}	
	}
	
	public void deleteRecords(RecordQueryOption query) throws Exception {
		
		try {	
			RecordProvider.deleteRecords(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), query);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}		
	}
		
	public void deleteRecords(byte[] startKey, byte[] endKey) throws Exception {
		
		try {	
			RecordProvider.deleteRecordsByRange(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), startKey, endKey);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}		
	}
	
	public void deleteRecords(Long startTime, Long endTime) throws Exception {
		
		try {	
			RecordProvider.deleteRecordsByTimeRange(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), startTime, endTime);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}		
	}
	
	public void deleteRecords(byte[] startKey, byte[] endKey, RecordQueryOption query) throws Exception {
		
		try {	
			RecordProvider.deleteRecordsByRangeWithFilter(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), startKey, endKey, query);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to delete records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}			
	}	

	public RecordResult getRecords(byte[] startKey, byte[] endKey, RecordQueryOption query) throws OtsException, TableException, ConfigException {

		try {
			return RecordProvider.getRecordsByRange(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())),
					startKey, endKey, query);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to query records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to query records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,	"Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to query records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (DecoderException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query records!\n" + e.getMessage());
		} 
	}
	
	
	public RecordResult getRecords(RecordQueryOption query) throws OtsException, TableException, ConfigException {

		try {
			return RecordProvider.getRecords(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), query);

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to query records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to query records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,	"Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to query records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (DecoderException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query records!\n" + e.getMessage());
		}
	}
	
	public RecordResult getRecord(byte[] key, List<byte[]> columns, Map<String, byte[]> hbase_attributes) throws OtsException, TableException, ConfigException {
		List<byte[]> keys = new ArrayList<byte[]>();
		keys.add(key);
		RecordQueryOption query = new RecordQueryOption(columns, 1L, 0L, null, false, null, hbase_attributes);
		
		return getRecords(keys, query);
	}
	
	public RecordResult getRecords(List<byte[]> keys, RecordQueryOption query) throws OtsException, TableException, ConfigException {
	
		try {	
			return RecordProvider.getRecordsByKeys(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), keys, query);					
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error("Failed to query records because hbase master no running!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to query records because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error("Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,	"Failed to query because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to query records!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query records!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		}	
	}
	
	public byte[] getRecordFile(byte[] rowkey, byte[] column) throws TableException, OtsException, Exception {
		try {
			return RecordProvider.getRecordFile(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), rowkey, column);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to get record file!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_GET, "Failed to get record file!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to get record file !\n" + e.getMessage());			
			throw e;
		}
	}
	
	public void putRecordFile(byte[] rowkey, byte[] column, byte[] data) throws TableException, OtsException {
		try {
			RecordProvider.putRecordFile(ConnectionUtil.getInstance().getTable(getTableFullname(getTenantid(), getName())), rowkey, column, data);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Failed to get record file!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_INSERT, "Failed to get record file!\n" + e.getMessage());
		} catch (TableException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Failed to get record file!\n" + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_INSERT, "Failed to get record file!\n" + e.getMessage());
		} 		
	}
	
	//////////////////////////metrics
	public long getSize() throws Exception {
		Admin admin = null;
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			return MetricsProvider.getTableMetrics(admin, getTenantidAsString(), getName()).getlStorefileSize();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public long getReadCount() throws Exception {
		Admin admin = null;
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			return MetricsProvider.getTableMetrics(admin, getTenantidAsString(), getName()).getlReadRequestsCount();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public long getWriteCount() throws Exception {
		Admin admin = null;
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			return MetricsProvider.getTableMetrics(admin, getTenantidAsString(), getName()).getlWriteRequestsCount();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
		
	private void addCorprocessors(HTableDescriptor htd) throws IOException {
		htd.addCoprocessor("com.baosight.xinsight.ots.coprocessor.IndexRegionObserver");
	}
	  
	public OtsSecondaryIndex createSecordaryIndex(SecondaryIndexInfo secIndex) throws SecondaryIndexException, InterruptedException, IndexException, ConfigException, NumberFormatException, com.baosight.xinsight.ots.exception.OtsException, IOException{
	    Admin admin = null;
	    
	    if(secIndex.getColumns().size() <= 0) {
	    	throw new SecondaryIndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NUM, "Failed to create index, because lack index column def!");
	    }
	    
		//check duplicate columns and empty name
		if (secIndex.checkColumnsDuplicateAndEmpty()) {
			LOG.warn("create index "+ getName() + "." + secIndex.getName() + " failed for duplicate column or empty column name!");
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_DUPLICATE_COLUMN, "create index "+ getName() + "." + secIndex.getName() + " failed for duplicate column or empty column name!");
		}
		
	    String tableFullName = getTableFullname(getTenantid(), getName());
	    TableName userTable = TableName.valueOf(tableFullName);
	    TableName indexTableName = TableName.valueOf(SecondaryIndexUtil.getIndexTableName(tableFullName, secIndex.getName()));
		Configurator configurator = new Configurator();
	    long idxid = 0;
	    
		try {
			// add to rdb
			Index index = new Index();
			index.setName(secIndex.getName());
			index.setShardNum(0);
			index.setReplicationNum(0);
			index.setTid(getId());
			index.setUid(getUserid());
			index.setCreateTime(new Date());
			index.setLastModify(new Date());
			
			SecondaryIndexColumnListConvert convert = new SecondaryIndexColumnListConvert();
			convert.setColumnList(secIndex.getColumns());
			index.setIndexColumns(convert.toString());			
			index.setPattern((char)OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX);
			
			admin = ConnectionUtil.getInstance().getAdmin();
	    	HTableDescriptor userTableDescriptor = admin.getTableDescriptor(userTable);
        	HColumnDescriptor userTableColumnDescriptor = userTableDescriptor.getFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));

	    	if(0 == userTableDescriptor.getCoprocessors().size()) {
		    	TableProvider.setTableEnableStatus(admin, userTable, false);
	    		addCorprocessors(userTableDescriptor);
		    	TableProvider.setTableEnableStatus(admin, userTable, true);
	    	}
	    	        	
	    	List<SecondaryIndexInfo>  indexes = SecondaryIndexUtil.parseIndexes(userTableDescriptor.getValue(OtsConstants.OTS_INDEXES));
	    	if(indexes.size() > 0 && SecondaryIndexUtil.cotainIndex(indexes, secIndex.getName())) {
	    		throw new SecondaryIndexException(OtsErrorCode.EC_OTS_SEC_INDEX_ALREADY_EXIST, "Index already exist!");
	    	}
	    	
	    	if(hasSecondaryIndexBuilding()) {
	    		throw new SecondaryIndexException( OtsErrorCode.EC_OTS_SEC_INDEX_INDEX_IS_BUILDING, 
	    				"Failed to delete secondary index, because some secondary index of table is building, please try again!");
	    	}	    	

	    	indexes.add(secIndex);
	    	
			//after rdb success, add hbase info; if hbase operate error, clean rdb info		
	    	String newOtsIndexes = SecondaryIndexUtil.indexesToString(indexes);
	    	SecondaryIndexUtil.parseIndexes(newOtsIndexes);
	    	userTableDescriptor.setValue(OtsConstants.OTS_INDEXES, newOtsIndexes);	    
	    	
		    //modifies the existing table's descriptor.	    	
			idxid = configurator.addIndex(index);
	    	admin.modifyTable(userTable, userTableDescriptor);
	    	
			HTableDescriptor indexTableDesc = new HTableDescriptor(indexTableName);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
							
			columnDescriptor.setScope(OtsConstants.DEFAULT_REPLICATED_SCOPE);		        
			columnDescriptor.setCompressionType(userTableColumnDescriptor.getCompressionType());
			columnDescriptor.setMaxVersions(1);        	
			//columnDescriptor.setMobEnabled(userTableColumnDescriptor.isMobEnabled());
			//columnDescriptor.setMobThreshold(userTableColumnDescriptor.getMobThreshold());
	       
			indexTableDesc.addFamily(columnDescriptor);
			indexTableDesc.setMaxFileSize(Long.MAX_VALUE);
			indexTableDesc.setValue(HTableDescriptor.SPLIT_POLICY, "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");

			admin.createTable(indexTableDesc);		
	    	
	    	return new OtsSecondaryIndex(getId(), idxid, tableFullName, secIndex, this.conf);

		} catch (IOException e) {
    		try {
    			admin.getTableDescriptor(indexTableName);
    		} catch (TableNotFoundException tnfe) {
    			configurator.delIndex(idxid);
    		}
			throw new SecondaryIndexException(OtsErrorCode.EC_OTS_SEC_INDEX_CREATE_INDEX_TABLE_FAILED, e.getMessage());
			
		} finally {    	
	    	TableProvider.setTableEnableStatus(admin, userTable, true);

	    	try {
		    	if (admin != null) {
					admin.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}	    	

    		if(null != configurator) {
    			configurator.release();
    		}
		}
	}
	
	public boolean hasSecondaryIndexBuilding() throws IOException, ConfigException {	
		Configurator configurator = new Configurator();

		try {
			YarnAppUtil yarnAppUtil = new YarnAppUtil(conf.getProperty(com.baosight.xinsight.config.ConfigConstants.YARN_RM_HOST), 
	    			conf.getProperty(com.baosight.xinsight.config.ConfigConstants.REDIS_QUORUM));
			List<Index> indexes = configurator.queryTableIndex(tenantid, getName());
	    	for(Index index:indexes) {
	    		if(SecondaryIndexUtil.isIndexBuilding(yarnAppUtil, getId(), getName(), index.getId(), index.getName())) {
	    			return true;
	    		}
	    	}
	    	
	    	return false;
	    	
		} finally {
			configurator.release();
		}    	
	}
	
	public void rebuildSecondIndex(String indexname) throws IOException, SecondaryIndexException, ConfigException, InterruptedException, OtsException {
		Configurator configurator = new Configurator();

		try {
			OtsSecondaryIndex secIndex = getSecondaryIndex(indexname);
			if(null != secIndex) {
				secIndex.rebuild(OtsConstants.OTS_MAPREDUCE_JAR_PATH, true);
							
				//update rdb index info
		    	Index indexInfo = new Index();
		    	indexInfo.setTid(getId());
		    	indexInfo.setName(indexname);
		    	indexInfo.setLastModify(new Date());
				
				configurator.updateIndexTime(indexInfo);	
			}
			else
			{
				LOG.error("Update index failed, " + tablename + "." + indexname +" no exist!");
				throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_UPDATE_INDEX_TABLE_FAILED, "Update index failed, " + tablename + "." + indexname +" no exist!");					
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Update index failed, " + tablename + "." + indexname);
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_UPDATE_INDEX_TABLE_FAILED, "Update index failed, " + tablename + "." + indexname);					
		} finally {
			configurator.release();
		}	
	}
	
	public void truncateSecondIndex(String indexname) throws IOException, SecondaryIndexException, ConfigException, InterruptedException, OtsException {
		Configurator configurator = new Configurator();

		try {
			OtsSecondaryIndex secIndex = getSecondaryIndex(indexname);
			if(null != secIndex) {
				secIndex.truncate();			
				
				//update rdb index info
		    	Index indexInfo = new Index();
		    	indexInfo.setTid(getId());
		    	indexInfo.setName(indexname);
		    	indexInfo.setLastModify(new Date());
				
				configurator.updateIndexTime(indexInfo);	
			}
			else {
				LOG.error("Update index failed, " + tablename + "." + indexname +" no exist!");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, "Update index failed, " + tablename + "." + indexname +" no exist!");					
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Update index failed, " + tablename + "." + indexname);
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_UPDATE_INDEX_TABLE_FAILED, "Update index failed, " + tablename + "." + indexname);					
		} finally {
			configurator.release();
		}	
	}
	
	public void deleteSecordaryIndex(String indexName) throws OtsException, SecondaryIndexException, ConfigException, IOException, InterruptedException 
	{
	    Admin admin = null;
	    Configurator configurator = null;
		String tableFullName = getTableFullname(getTenantid(), getName());
	    TableName userTable = TableName.valueOf(tableFullName);
    	TableName indexTable = TableName.valueOf(SecondaryIndexUtil.getIndexTableName(tableFullName, indexName));
    	List<SecondaryIndexInfo> indexes = null;
		YarnAppUtil yarnAppUtil = new YarnAppUtil(conf.getProperty(com.baosight.xinsight.config.ConfigConstants.YARN_RM_HOST), 
    			conf.getProperty(com.baosight.xinsight.config.ConfigConstants.REDIS_QUORUM));
    	
    	try {
    		configurator = new Configurator();
	        Index index = configurator.queryIndex(tenantid, getName(), indexName);
	        if(null == index || index.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX) {
	        	throw new SecondaryIndexException(OtsErrorCode.EC_OTS_SEC_INDEX_ALREADY_EXIST, "Index no exist!");
	        }
	        
	    	if(hasSecondaryIndexBuilding()) {
	    		throw new SecondaryIndexException(OtsErrorCode.EC_OTS_SEC_INDEX_INDEX_IS_BUILDING, "Failed to delete secondary index, because some secondary index of table is building, please try again!");
	    	}	
	        
			admin = ConnectionUtil.getInstance().getAdmin();
	    	HTableDescriptor userTableDescriptor = admin.getTableDescriptor(userTable);
	    	indexes = SecondaryIndexUtil.parseIndexes(userTableDescriptor.getValue(OtsConstants.OTS_INDEXES));  
	    	
	    	for(int i = 0; i < indexes.size(); ++i) {
				if(indexes.get(i).getName().equals(indexName)) {
					indexes.remove(i);
				}
	        }	    	
	        userTableDescriptor.setValue(OtsConstants.OTS_INDEXES, SecondaryIndexUtil.indexesToString(indexes));	        
	        admin.modifyTable(userTable, userTableDescriptor);
	        
	        TableProvider.deleteTable(admin, indexTable);	  
	        
	    	yarnAppUtil.delApp(YarnTagGenerator.GenSecIndexBuildMapreduceTag(getId(), tableFullName, index.getId(), indexName));
	          
	    	configurator.delIndex(getId(), indexName);
	    	configurator.disConnect();	        
		} catch (IOException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_FAILED_DELETE, "Failed to delete index " + indexName + "!" + e.getMessage());
		} finally {
	    	try {
		    	if (admin != null) {
					admin.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}	
	    	
    		if(null != configurator) {
    			configurator.release();
    		}
    	}
	}
	
	public OtsSecondaryIndex getSecondaryIndex(String name) throws IOException, OtsException, ConfigException
	{
	    Admin admin = null;
	    Configurator configurator = new Configurator();
	    Index index = null;
	    OtsSecondaryIndex secondaryIndex = null;
	    
		try {
			index = configurator.queryIndex(tenantid, getName(), name);
			if (index == null) {
				throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_FAILED_GET_INDEX_INFO, "Failed to get index " + name + "!");
			}
		} catch (ConfigException e) {
			throw e;
		} finally {
		    configurator.release();			
		}

		try {
			String tableFullName = getTableFullname(getTenantid(), getName());
		    TableName userTable = TableName.valueOf(tableFullName);
	    	admin = ConnectionUtil.getInstance().getAdmin();
	    	
	    	HTableDescriptor userTableDescriptor = admin.getTableDescriptor(userTable);
	    	List<SecondaryIndexInfo>  indexes = SecondaryIndexUtil.parseIndexes(userTableDescriptor.getValue(OtsConstants.OTS_INDEXES));
	    	for(SecondaryIndexInfo secIndex:indexes) {
	    		if(secIndex.getName().equals(name))	{
	    			secondaryIndex = new OtsSecondaryIndex(getId(), index.getId(),tableFullName, secIndex, this.conf);
	    			break;
	    		}
	    	}	    	
	    	
	    	return secondaryIndex;

		} finally {
	    	try {
		    	if (admin != null) {
					admin.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
	    }		
	}	
}