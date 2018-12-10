package com.baosight.xinsight.ots.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.secondaryindex.SecondIndexQueryOption;
import com.baosight.xinsight.ots.client.secondaryindex.SecondIndexQueryOption.ColumnRange;
import com.baosight.xinsight.ots.client.table.RecordProvider;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.table.RowRecord;
import com.baosight.xinsight.ots.client.util.ConnectionUtil;
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn.ValueType;
import com.baosight.xinsight.ots.common.util.SecondaryIndexUtil;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.yarn.YarnAppUtil;
import com.baosight.xinsight.yarn.YarnTagGenerator;

public class OtsSecondaryIndex {
	private static final Logger LOG = Logger.getLogger(OtsSecondaryIndex.class);
	
	private SecondaryIndexInfo indexInfo;
	private String tableName;
	private String indexTableName;
	private OtsConfiguration conf;
	private long idxid;
	private long tableid;
	
	private class OtsHbaseRowKey implements Comparable<OtsHbaseRowKey> {
		public byte[] rowKey;

		public OtsHbaseRowKey(byte[] rowKey) {
			this.rowKey = rowKey;
		}
		
		@Override
		public int compareTo(OtsHbaseRowKey o) {
			return Bytes.compareTo(rowKey, o.rowKey);
		}
	}
	
	public OtsSecondaryIndex(long tableid, long idxid, String tableName, SecondaryIndexInfo indexInfo, OtsConfiguration conf)
	{
		this.indexInfo = indexInfo;
		this.tableName = tableName;
		indexTableName = SecondaryIndexUtil.getIndexTableName(tableName, indexInfo.getName());
		this.conf = conf;
		this.idxid = idxid;
		this.tableid = tableid;
	}
	
	public SecondaryIndexInfo getIndexInfo() {
		return indexInfo;
	}
	
	private byte[] getUserTableKeyFromIndexKey(byte[] indexKey) throws OtsException {
		int indexPreKeyLen = indexInfo.getKeyLength();
		int rowKeyLen = indexKey.length - indexPreKeyLen;
		byte[] userTableKey = new byte[rowKeyLen];
		System.arraycopy(indexKey, indexPreKeyLen, userTableKey, 0, rowKeyLen);
		
		return userTableKey;
	}
	
	private byte[] getColumnFromIndexKey(byte[] indexKey, int offset, int length,ValueType type) {
		byte[] column = new byte[length];
		System.arraycopy(indexKey, offset, column, 0, length);
		switch(type)
		{
		  case string:
				return Bytes.toBytes(Bytes.toString(column).trim());	 
		  default:
				return column;	  
		}
	}
	
	private byte[] cellValueToByteValue(ValueType type, byte[] cellValue) {
		byte[] realValue = null;
		  switch(type)
		  {
		  case string:
		  case binary:	
			  realValue =  cellValue;	
			  break;
		  case int32:
			  realValue = Bytes.toBytes(Integer.parseInt(Bytes.toString(cellValue)));
			  break;
		  case float32:
			  realValue = Bytes.toBytes(Float.parseFloat(Bytes.toString(cellValue)));
			  break;
		  case float64:
			  realValue = Bytes.toBytes(Double.parseDouble(Bytes.toString(cellValue)));				  
			  break;
		  case int64:
			  realValue = Bytes.toBytes(Long.parseLong(Bytes.toString(cellValue)));
			  break;
		  default:
			  break;
		  }
		  
		  return realValue;
	}
	
	// check key in range
	private boolean checkKey(byte[] key, byte[] startKey, byte[] stopKey) {
		byte[] hashKey = PrimaryKeyUtil.getHashKey(key);
		byte[] rangeKey = PrimaryKeyUtil.getRangeKey(key);
		
		if(startKey != null) { // only hash		
			if(0 == rangeKey.length) {//table only has hash				
				if(0 != Bytes.compareTo(key, startKey)) {
					return false;	
				}
			}
			else {
				byte[] startHash = PrimaryKeyUtil.getHashKey(startKey);
				byte[] startRange = PrimaryKeyUtil.getRangeKey(startKey);
				if(0 != Bytes.compareTo(startHash, hashKey)) {
					return false;
				}
				if(startRange.length > 0 && Bytes.compareTo(rangeKey, startRange) < 0) {
					return false;
				}
			}
		}

		if(stopKey != null) {
			if(0 == rangeKey.length) {
				if(0 != Bytes.compareTo(key, stopKey)) {
					return false;		
				}
			}
			else {
				byte[] stopHash = PrimaryKeyUtil.getHashKey(stopKey);
				byte[] stopRange = PrimaryKeyUtil.getRangeKey(stopKey);
				if(0 != Bytes.compareTo(stopHash, hashKey)) {
					return false;
				}
				if(stopRange.length > 0) {
					if(Bytes.compareTo(rangeKey, stopRange) > 0) {
						return false;
					}
					if(Bytes.compareTo(rangeKey, stopRange) == 0 && 0 != Bytes.compareTo(startKey, stopKey)) {
						return false;
					}						
				}
			}
		}	
		
		return true;
	}
	
	// check value in [startRange, stopRange) or not
	private boolean checkColumnValueByRange(byte[] value, byte[] startRange, byte[] stopRange) {
		if((startRange == null || Bytes.compareTo(value, startRange)>=0) 
				&& (stopRange == null || Bytes.compareTo(value, stopRange) < 0 
					|| (0 == Bytes.compareTo(value, stopRange) && (null == startRange? false:0 == Bytes.compareTo(startRange, stopRange))))) {
			return true;
		}		
		
		return false;
	}
	
	private boolean checkColumnValues(byte[] indexRow, SecondIndexQueryOption query) throws OtsException {
		List<SecondaryIndexColumn> indexColumns = indexInfo.getColumns();
		for(SecondaryIndexColumn indexColumn:indexColumns) {
			ColumnRange range = query.getRanges().get(indexColumn.getName());
			if(null != range) {
				byte[] columnValue = getColumnFromIndexKey(indexRow, indexInfo.getColumnKeyOffset(indexColumn.getName()), 
						indexColumn.getMaxLen(), indexColumn.getType());

				if(!checkColumnValueByRange(columnValue, range.getStart(), range.getStop())) {
					return false;
				}
			}
		}		
		
		return true;
	}
	
	private boolean checkColumnValues(Result result, SecondIndexQueryOption query) {
		for(SecondaryIndexColumn indexColumn:indexInfo.getColumns()) {
			ColumnRange range = query.getRanges().get(indexColumn.getName());
			if(null != range) {
				Cell cell = result.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes(indexColumn.getName()));
				byte[] columnValue = this.cellValueToByteValue(indexColumn.getType(), CellUtil.cloneValue(cell));
				if(!checkColumnValueByRange(columnValue, range.getStart(), range.getStop())) {
					return false;
				}
			}
		}		
		
		return true;
	}
	
	private List<Get> TransofrmRowKeySetToGetList(Set<OtsHbaseRowKey> rowKeys, List<byte[]> queryColumns) {
		List<Get> gets = new ArrayList<Get>();
		for(OtsHbaseRowKey rowKey:rowKeys) {
			Get get = new Get(rowKey.rowKey);
			for(byte[] queryColumn:queryColumns) {
				get.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), queryColumn);
			}			
			gets.add(get);
		}	
		
		return gets;
	}
	
	public RecordResult getRecords(SecondIndexQueryOption query) throws DecoderException, IOException, OtsException {
		Table indexTable = null;
		Table userTable = null;
		ResultScanner scanner = null;
		List<RowRecord> recordList = new ArrayList<RowRecord>();
		Long matchCounter = 0L;
		int curOffset = 0;
		
		try {
			LOG.debug(Bytes.toStringBinary(query.getStartRowKey()));
			LOG.debug(Bytes.toStringBinary(query.getStopRowKey()));
			
			Scan scan = new Scan();
			scan.setStartRow(query.getStartRowKey());
			scan.setStopRow(query.getStopRowKey());

			indexTable = ConnectionUtil.getInstance().getTable(indexTableName);
			scanner = indexTable.getScanner(scan);
			Result tmpResult = null;
			userTable = ConnectionUtil.getInstance().getTable(tableName);
			Set<OtsHbaseRowKey> matchRowKeys = new TreeSet<OtsHbaseRowKey>();

			while(null != (tmpResult = scanner.next()) && matchCounter < query.getLimit())	{
				boolean needAdd = true;
				// check row key
				byte[] indexRow = tmpResult.getRow();
				byte[] userTableRow = getUserTableKeyFromIndexKey(indexRow);
				if(!checkKey(userTableRow, query.getStartUserTableKey(), query.getStopUserTableKey())) {
					continue;
				}
				
				needAdd = checkColumnValues(indexRow, query);			
				if(needAdd) {
					matchRowKeys.add(new OtsHbaseRowKey(userTableRow));
					matchCounter++;
					
					if(matchCounter >= query.getLimit()) {
						List<Get> gets = TransofrmRowKeySetToGetList(matchRowKeys, query.getColumns());
						
						Result[] results = userTable.get(gets);
						//long count = 0;
						boolean onlyRowkey = query.onlyGetRowKey();
						for (Result relt : results) {
							if (relt != null && !relt.isEmpty()) {
								if(checkColumnValues(relt, query)) {
									curOffset++;
									if(curOffset <= query.getOffset()) {
										continue;
									}
									//count++;
									RowRecord recModel = RecordProvider.readRow(onlyRowkey, relt);
									recordList.add(recModel);				
								}
							}
						}				
						
						matchCounter = new Long(recordList.size());
						if(matchCounter == query.getLimit()) {
							tmpResult = scanner.next();
							if(null != tmpResult) {
								query.updateCursor(tmpResult.getRow());
							}
							else {
								query.setEnd();
							}							
						}
						gets.clear();
						matchRowKeys.clear();
					}
				}
			}
			
			if(matchRowKeys.size() > 0) {
				List<Get> gets = TransofrmRowKeySetToGetList(matchRowKeys, query.getColumns());				
				Result[] results = userTable.get(gets);
				//long count = 0;
				boolean onlyRowkey = query.onlyGetRowKey();
				for (Result relt : results) {
					if (relt != null && !relt.isEmpty()) {							
						if(checkColumnValues(relt, query)) {
							curOffset++;
							if(curOffset <= query.getOffset()) {
								continue;
							}
							//count++;
							RowRecord recModel = RecordProvider.readRow(onlyRowkey, relt);
							recordList.add(recModel);							
						}
					}
				}				
				
				matchCounter = new Long(recordList.size());
				query.setEnd();
				gets.clear();
				matchRowKeys.clear();
			}
			
			return new RecordResult(matchCounter, query.getCursor(), recordList);
			
		} finally {
			if (scanner != null) {
				scanner.close();  
			}
			
			try {
				if (indexTable != null) {
					indexTable.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				if (userTable != null) {
					userTable.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}
	
	public void rebuild(String mapreduceJarPath, boolean needTruncate) throws IOException, InterruptedException, IndexException {
    	
		StringBuilder cmd = new StringBuilder();
		YarnAppUtil yarnAppUtil = new YarnAppUtil(conf.getProperty(com.baosight.xinsight.config.ConfigConstants.YARN_RM_HOST), 
    			conf.getProperty(com.baosight.xinsight.config.ConfigConstants.REDIS_QUORUM));
		
    	if(!SecondaryIndexUtil.isIndexBuilding(yarnAppUtil, tableid, tableName, idxid, indexInfo.getName())) {
    		if(needTruncate) {
        		truncate();			
    		}

    		Admin admin = null;
    		try {
    			admin = ConnectionUtil.getInstance().getAdmin();
        		TableName userTable = TableName.valueOf(tableName);
        	  	if(admin.isTableDisabled(userTable)) {
        	  		throw new IndexException(OtsErrorCode.EC_OTS_SEC_INDEX_INDEX_TABLE_DISABLED, "Failed to rebuild,because user table disabled!");
        	  	}
			} finally {
		    	try {
			    	if (admin != null) {
						admin.close();
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
		    }
    		
        	cmd.append("nohup hadoop jar ");
        	cmd.append(mapreduceJarPath);
        	cmd.append(" com.baosight.xinsight.ots.mapreduce.SecondaryIndex.BuildMapReduce -zk " );
        	cmd.append(conf.getProperty(OtsConstants.ZOOKEEPER_QUORUM));
        	cmd.append(" -tid ");
        	cmd.append(tableid);
        	cmd.append(" -table ");
        	cmd.append(tableName);
        	cmd.append(" -iid ");
        	cmd.append(idxid);
        	cmd.append(" -index ");
        	cmd.append(indexInfo.getName());

        	System.out.println("rebuild comand: " + cmd);
        	String[] cmds = new String[]{"/bin/sh", "-c", cmd.toString()};
        	//Process ps = 
        	Runtime.getRuntime().exec(cmds);
        	
        	yarnAppUtil.addApp(YarnTagGenerator.GenSecIndexBuildMapreduceTag(tableid, tableName, idxid, indexInfo.getName()));   		
    	}
    	else {
    		throw new IndexException(OtsErrorCode.EC_OTS_SEC_INDEX_INDEX_IS_BUILDING, "Can not to rebuild index, last rebuild don't end!");
    	}

    	//ps.waitFor();	
	}
	
	public void truncate() throws IOException, InterruptedException {
		
		Admin admin = null;
		int timeout = 5000;

		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			TableName indexTable = TableName.valueOf(indexTableName);
			if(admin.isTableEnabled(indexTable)) {
				admin.disableTable(indexTable);
			}
			
			while(admin.isTableEnabled(indexTable)) {
				admin.disableTable(indexTable);
				if(timeout > 0) {
					Thread.sleep(1000);
					timeout -= 1000;				
				}
				else
				{
					break;
				}
			}
			
			admin.truncateTable(indexTable, false);
			
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
