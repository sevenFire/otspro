package com.baosight.xinsight.ots.client.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import com.baosight.xinsight.ots.client.OtsAdmin;
import com.baosight.xinsight.utils.BytesUtil;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.filter.RegexStringComparator;
//import org.apache.hadoop.hbase.filter.RowFilter;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.TableException;


public class RecordProvider {	
	
	/**
	 * getRecords by Range Or by RangewithExp获取表数据
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * @throws DecoderException 
	 *  
	 */
    public static RecordResult getRecordsByRange(Table table, byte[] startKey, byte[] endKey, RecordQueryOption model) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, DecoderException  {
		
		if (startKey == null && endKey == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_RECQUERY_RANGE, "Error RecordRangeQuery param!");
		}
		
		if (!model.isValidPage()) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Error query Recordkey param, need valid iterate param!");
		}
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		
		if (model.getFilter() != null){
			scan.setFilter(model.getFilter());
		}
		
		//add hbase attribute option, 2015-11-17
		if (model.getHbase_attributes() != null) {
			for (String attribute : model.getHbase_attributes().keySet()) {
				scan.setAttribute(attribute, model.getHbase_attributes().get(attribute));
			}
		}
		
		//Cursor mark
		byte[] realStartKey = startKey;
		if (model.hasIterate()) {
			if (!model.getCursor_mark().equals(OtsConstants.DEFAULT_QUERY_CURSOR_START)) {
				realStartKey = Hex.decodeHex(model.getCursor_mark().toCharArray());
				scan.setStartRow(realStartKey==null?HConstants.EMPTY_START_ROW:realStartKey);//important
			}
		}
		
		if (model.hasCaching()){
			scan.setCaching(model.getCaching());
		}
		
		//默认降序,如果升序的话需要反转scan
		if (model.isDescending()) {
			scan.setReversed(true);//important
			scan.setStartRow(endKey==null?HConstants.EMPTY_END_ROW:endKey);
			scan.setStopRow(realStartKey==null?HConstants.EMPTY_START_ROW:realStartKey);
		} else {
			scan.setStartRow(realStartKey==null?HConstants.EMPTY_START_ROW:realStartKey);
			scan.setStopRow(endKey==null?HConstants.EMPTY_END_ROW:endKey);
		}
			
		if (model.hasColumns()) {
			List<byte[]> columns = model.getColumns();		
			boolean onlyRowkey = model.onlyGetRowKey();
			if (!onlyRowkey) {						
				for (byte[] col : columns) {
					//if (0 != Bytes.compareTo(col, Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
						scan.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), col);
					//}
				}
				scan.setBatch(columns.size()); //*HM 2017-01-04
			}
		}

		if (model.hasIterate()) {
			return queryRecordsByCursorMark(table, model, scan);
		} else {
			return queryRecords(table, model, scan);
		}
	}
    
	/**
	 * RecordKeysQuery获取表数据
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * 
	 */
	public static RecordResult getRecordsByKeys(Table table, List<byte[]> keys, RecordQueryOption model) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
         class ByteComapre{
        	public int compare(byte[] left, byte[] right){
        		if (left == right)
        			return 0;
        		for (int i = 0,j = 0; i < left.length && j < right.length; i++,j++){
        			int nLeft = (left[i]&0xff);
        			int nRight = (right[j]&0xff);
        			if (nLeft != nRight)
        				return nLeft - nRight;
        		}
        		return left.length - right.length;
        	}
        }
		if (keys == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_RECQUERY_KEY, "Error RecordKeysQuery param!");
		}

		List<RowRecord> recordList = new ArrayList<RowRecord>();
		long matchCounter = 0;
		boolean onlyRowkey = model.onlyGetRowKey();
		if (keys.size() > 0) {	//has valid key		
			
			if (model.getIsSort()) {  //if sort, check whether desc or asc
				if (model.isDescending()) {
					Collections.sort(keys,new Comparator<byte[]>(){   
				           public int compare(byte[] arg0, byte[] arg1) {   
				               return new ByteComapre().compare(arg1, arg0);
				            }   
				        });   
				} else {
					Collections.sort(keys,new Comparator<byte[]>(){   
				           public int compare(byte[] arg0, byte[] arg1) {   
				               return new ByteComapre().compare(arg0, arg1);
				            }   
				        });   
				}
			}
	
			List<Get> listGets = new ArrayList<Get>();
			for (byte[] row : keys) {
				Get get = new Get(row);
				get.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
				
				//add hbase attribute option, 2015-11-17
				if (model.getHbase_attributes() != null) {
					for (String attribute : model.getHbase_attributes().keySet()) {
						get.setAttribute(attribute, model.getHbase_attributes().get(attribute));
					}
				}
				
				// add columns if request
				if (model.hasColumns()) {
					List<byte[]> columns = model.getColumns();
					if (!onlyRowkey) {						
						for (byte[] col : columns) {
							//if (0 != Bytes.compareTo(col, Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
								get.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), col);
							//}
						}
					}
				}
				
				listGets.add(get);
			}
		
			try {
				Result[] results = table.get(listGets);
				for (Result relt : results) {
					if (relt != null && !relt.isEmpty()) {
						matchCounter++;					
					
						RowRecord recModel = readRow(onlyRowkey, relt);
						recordList.add(recModel);
					}
				}
	
			} catch (IOException e) {
				e.printStackTrace();
				
				if ( !(e instanceof DoNotRetryIOException)) {
					
					if (e.getCause() instanceof NotServingRegionException) {
						throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
					} else if (e.getCause() instanceof RegionTooBusyException) {
						throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
					}
				} 			
				throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "query Record error (by keys)!");
				
			} finally {
				table.close();
			}
		}
		
		return new RecordResult(matchCounter, recordList);
	}
	
	public static byte[] getRecordFile(Table table, byte[] row, byte[] column) throws TableException, IOException {
		byte[] data = null;
		Get get = new Get(row);
		get.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		get.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), column);
		
		try {
			Result result = table.get(get);
			Cell cell = result.getColumnLatestCell(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), column);
			data= CellUtil.cloneValue(cell);
		} catch (IOException e) {
			e.printStackTrace();
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_GET, "get Record file error!");
		} finally {
			table.close();
		}

		return data;
	}
	
	public static void putRecordFile(Table table, byte[] row, byte[] column, byte[] data) throws TableException, IOException {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), column, data);
		
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_INSERT, "upload Record file error!");
		} finally {
			table.close();
		}
	}

	/**
	 * 通过RecordKeyQuery with filter, for example regexExp 获取表数据
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * @throws DecoderException 
	 * 
	 */
//	public static RecordResult getRecords(Table table, Long tableId, RecordQueryOption model) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, DecoderException  {
	public static RecordResult getRecords(Table table, RecordQueryOption model) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, DecoderException  {
		if (model == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error query Recordkey param!");
		}
		
		if (!model.isValidPage()) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Error query Recordkey param, need valid iterate param!");
		}
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		
		//add hbase attribute option, 2015-11-17
		if (model.getHbase_attributes() != null) {
			for (String attribute : model.getHbase_attributes().keySet()) {
				scan.setAttribute(attribute, model.getHbase_attributes().get(attribute));
			}
		}
		
		if (model.isDescending()) {
			scan.setReversed(true);//important
		}
		
		if (model.hasCaching()){
			scan.setCaching(model.getCaching());
		}
		
		//Cursor mark
		if (model.hasIterate()) {
			if (!model.getCursor_mark().equals(OtsConstants.DEFAULT_QUERY_CURSOR_START)) {
				byte[] realStartKey = Hex.decodeHex(model.getCursor_mark().toCharArray());
				scan.setStartRow(realStartKey==null?HConstants.EMPTY_START_ROW:realStartKey);//important
			}
		}
			
		try {
			//add RowFilter, for example RegexStringComparator
			if (model.getFilter() != null) {
				Filter regexFilter = model.getFilter();
				scan.setFilter(regexFilter);
			}
		} catch (PatternSyntaxException e) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error RegexStringComparator param!");
		}
			
		if (model.hasColumns()) {
			List<byte[]> columns = model.getColumns();
			boolean onlyRowkey = model.onlyGetRowKey();
			if (!onlyRowkey) {	
				for (byte[] col : columns) {
					//if (0 != Bytes.compareTo(col, Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
						scan.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), col);
					//}
				}
				scan.setBatch(columns.size()); //*HM 2017-01-04
			}
		}			
		
		if (model.hasIterate()) {
			return queryRecordsByCursorMark(table, model, scan);
		} else {
			return queryRecords(table, model, scan);
		}
	}
	
	private static RecordResult queryRecords(Table table, RecordQueryOption model, Scan scan) throws IOException, TableException  {     
		long counter = 0;
		long matchCounter = 0;
		List<RowRecord> recordList = new ArrayList<RowRecord>();
		ResultScanner rs = null;
		boolean onlyRowkey = model.onlyGetRowKey();
		byte[] last_rowkey = null;
	    
		try {
			rs = table.getScanner(scan);
			Result rec = rs.next();
			while (rec != null) {
				if (!Arrays.equals(last_rowkey, rec.getRow())) {
					counter++;
					last_rowkey = rec.getRow();
				}
				
				// Check if past offset
				if (model.hasOffset()) {
					if (counter <= model.getOffset()) {
						rec = rs.next();
						continue;
					}					
				}
								
				RowRecord recModel = readRow(onlyRowkey, rec);
				
				//limit
				if(counter <= model.getOffset() + model.getLimit()) {
					RowRecord latestRecModel = recordList.size() > 0 ? recordList.get(recordList.size() -1):null;
					if (latestRecModel != null && Arrays.equals(latestRecModel.getRowkey(), rec.getRow())) {
						for (RowCell c : recModel.getCellList()) {
							//if (c != null && !Arrays.equals(c.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
							if (c != null) {
								latestRecModel.addCell(c);
							}
						}						
					} else {						
						matchCounter++;
						recordList.add(recModel);
					}
				}				    	
				
				// Check limit  
				if(counter >= model.getOffset() + model.getLimit()) {
					break;
				}						     
			    
				rec = rs.next();
			}
		} catch (Exception e) {
			e.printStackTrace();
			
			if ( !(e instanceof DoNotRetryIOException)) {
				
				if (e.getCause() instanceof NotServingRegionException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
				} else if (e.getCause() instanceof RegionTooBusyException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
				}
			} 			
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "query Record error!");
			
		} finally {
			if (rs != null) {
				rs.close();  
			}
			table.close();
		}

		return new RecordResult(matchCounter, recordList);
	}
	
	
	private static RecordResult queryRecordsByCursorMark(Table table, RecordQueryOption model, Scan scan) throws IOException, TableException  {     
		long counter = 0;
		long matchCounter = 0;
		List<RowRecord> recordList = new ArrayList<RowRecord>();
		ResultScanner rs = null;
		boolean onlyRowkey = model.onlyGetRowKey();
		byte[] next_rowkey = null;
		byte[] last_rowkey = null;

		try {
			rs = table.getScanner(scan);
			Result rec = rs.next();
			while (rec != null) {
				if (!Arrays.equals(last_rowkey, rec.getRow())) {
					counter++;
					last_rowkey = rec.getRow();
				} 
				
				RowRecord recModel = readRow(onlyRowkey, rec);
				
				//limit
				if(counter <= model.getLimit()) {
					RowRecord latestRecModel = recordList.size() > 0 ? recordList.get(recordList.size() -1):null;
					if (latestRecModel != null && Arrays.equals(latestRecModel.getRowkey(), rec.getRow())) {
						for (RowCell c : recModel.getCellList()) {
							//if (c != null && !Arrays.equals(c.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
							if (c != null) {
								latestRecModel.addCell(c);
							}
						}						
					} else {						
						matchCounter++;
						recordList.add(recModel);
					}
				}				   	
				
				// Check limit  
				if(counter >= model.getLimit()) {					
					//calculate next row key
					rec = rs.next();
					if (rec != null)
						next_rowkey = rec.getRow();
					
					break;
				}						     
			    
				rec = rs.next();
			}
		} catch (Exception e) {
			e.printStackTrace();
			
			if ( !(e instanceof DoNotRetryIOException)) {
				
				if (e.getCause() instanceof NotServingRegionException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
				} else if (e.getCause() instanceof RegionTooBusyException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
				}
			} 			
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "query Record error!");
			
		} finally {
			if (rs != null) {
				rs.close();  
			}
			table.close();
		}

		String strNextRowkey = OtsConstants.DEFAULT_QUERY_CURSOR_START;
		if (next_rowkey != null) {
			strNextRowkey = Hex.encodeHexString(next_rowkey);
		}
		return new RecordResult(matchCounter, strNextRowkey, recordList);
	}

	public static RowRecord readRow(boolean onlyRowkey, Result rec) {
		RowRecord recModel = new RowRecord();
		boolean rowkey = false;
		for(Cell cell: rec.rawCells()) {
			if (cell != null) {
				
				if (!rowkey) {								
					//RowCell cellRow = new RowCell();
					//cellRow.setName(Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME));
					//cellRow.setValue(CellUtil.cloneRow(cell));
					
					//recModel.addCell(cellRow);
					recModel.setRowkey(CellUtil.cloneRow(cell));
					rowkey = true;
				}

				if (!onlyRowkey) {
					RowCell cellModel = new RowCell();
					cellModel.setName(CellUtil.cloneQualifier(cell));					
					cellModel.setValue(CellUtil.cloneValue(cell));							
					//cellModel.setTimestamp(cell.getTimestamp());
					
					recModel.addCell(cellModel);
				}
			}
		}
		
		return recModel;
	}
	
	
	/**
	 * 删除表数据
	 * 
	 * 批量删除数据有两种方式:
	 * 1. 使用协处理器:用org.apache.hadoop.hbase.coprocessor.example.TestBulkDeleteProtocol中invokeBulkDeleteProtocol
	 * 2. 使用本实现方式
	 * 
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws TableException 
	 */
	public static void deleteRecords(Table table, RecordQueryOption query) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
		
		if (query == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error delete Recordkey param!");
		}
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
				
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		//add RowFilter, for example RegexStringComparator
		if (query.getFilter() != null) {
			Filter regexFilter = query.getFilter();
			filterList.addFilter(regexFilter);
		}
		
		//reduce transfer data and network 
		Filter keyFilter = new KeyOnlyFilter();
		filterList.addFilter(keyFilter);
		scan.setFilter(filterList);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
		
		deleteRecords(table, scan);
	}
	

	/**
	 * by keys 
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * 
	 */
	public static void deleteRecordsByKeys(Table table, List<byte[]> keys) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
	    int bufferSize = OtsConstants.DEFAULT_META_SCANNER_CACHING; //avoid running out of memory
	    int realDelCount = 0;
	    
		try {
			int counter = 0;			
			
			List<Delete> listDeletes = new ArrayList<Delete>();
			for (byte[] row : keys) {
				
				Delete del = new Delete(row);
				del.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));						
				listDeletes.add(del);
				
				if (counter < bufferSize) {					
					counter++;
				} else {
					table.delete(listDeletes);
					listDeletes.clear();
					counter = 0;
				}
				
				realDelCount++;				
			}
			
			if (listDeletes.size() > 0) {
				table.delete(listDeletes);
				listDeletes.clear();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
			
			// no match record
			if (realDelCount == 0) {
				throw new TableException(OtsErrorCode.EC_OTS_STORAGE_DELETE_RECORD_NOMATCH, "delete Record error, no match record!");
			}
		}
	}
	
	/**
	 * byRange with filter, regex for example 删除表数据
	 * 
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 */	
	public static void deleteRecordsByRangeWithFilter(Table table, byte[] startKey, byte[] endKey, RecordQueryOption query) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		
		if (startKey == null && endKey == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error delete Recordkey param!");
		}		
		
		if (query == null ) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error delete Recordkey param!");
		}		
		 
		if (query.getFilter() == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_REGEX, "Error delete Recordkey param, need filter!");
		}
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		scan.setStartRow(startKey==null?HConstants.EMPTY_START_ROW:startKey);
		scan.setStopRow(endKey==null?HConstants.EMPTY_END_ROW:endKey);		
		
		//add RowFilter, for example RegexStringComparator
		if (query.getFilter() != null) {
			Filter regexFilter = query.getFilter();
			filterList.addFilter(regexFilter);
		}
		
		//reduce transfer data and network 
		Filter keyFilter = new KeyOnlyFilter();
		filterList.addFilter(keyFilter);
		scan.setFilter(filterList);
		
		deleteRecords(table, scan);
	}
	
	/**
	 * byRange删除表数据
	 * 
	 * 批量删除数据有两种方式:
	 * 1. 使用协处理器:用org.apache.hadoop.hbase.coprocessor.example.TestBulkDeleteProtocol中invokeBulkDeleteProtocol
	 * 2. 使用本实现方式
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 */
	public static void deleteRecordsByRange(Table table, byte[] startKey, byte[] endKey) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		
		if (startKey == null && endKey == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_RECDELETE_RANGE, "Error Recordkey param!");
		}		
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));

		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		scan.setStartRow(startKey==null?HConstants.EMPTY_START_ROW:startKey);
		scan.setStopRow(endKey==null?HConstants.EMPTY_END_ROW:endKey);
		
		//reduce transfer data and network 
		Filter keyFilter = new KeyOnlyFilter();
		filterList.addFilter(keyFilter);
		scan.setFilter(filterList);
		
		deleteRecords(table, scan);
	}
	
	/**
	 * by timerange 
	 * 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException 
	 * @throws TableException 
	 * 
	 */
	public static void deleteRecordsByTimeRange(Table table, Long startTime, Long endTime) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException  {
		if (endTime == null) {
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_RECDELETE_RANGE, "Error Record Time Range Param!");
		}
		if (startTime == null) {
			startTime = 0L;
		}
		
	    //int bufferSize = OtsConstants.DEFAULT_META_SCANNER_CACHING; //avoid running out of memory
		int bufferSize = 1000;
	    int counter = 0;
	    int realDelCount = 0;
	    ResultScanner rs = null;
	    
		try {
			List<Delete> listDeletes = new ArrayList<Delete>();
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
			scan.setTimeRange(startTime, endTime);
			rs = table.getScanner(scan);

		    Result rec = rs.next();
			while (rec != null) {
				
				listDeletes.add(new Delete(rec.getRow()));
				if (counter < bufferSize) {
					counter++;
				} else {
					table.delete(listDeletes);
					listDeletes.clear();
					counter = 0;
				}
				
				realDelCount++;
				rec = rs.next();
			}
			
			if (listDeletes.size() > 0) {
				table.delete(listDeletes);
				listDeletes.clear();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();  
			}
			table.close();
			
			// no match record
			if (realDelCount == 0) {
				throw new TableException(OtsErrorCode.EC_OTS_STORAGE_DELETE_RECORD_NOMATCH, "delete Record error, no match record!");
			}
		}
	}
	
	/**
	 * 删除记录
	 * 
	 * @param scan
	 * @return
	 * @throws IOException
	 * @throws TableException 
	 */
	private static void deleteRecords(Table table, Scan scan) throws IOException, TableException  {		
	    int bufferSize = OtsConstants.DEFAULT_META_SCANNER_CACHING; //avoid running out of memory
        
		ResultScanner rs = null;
	    List<Delete> deletes = new ArrayList<Delete>();
	    int counter = 0;
	    int realDelCount = 0;
	    
		try {
			rs = table.getScanner(scan);

		    Result rec = rs.next();
			while (rec != null) {
				
				deletes.add(new Delete(rec.getRow()));
				if (counter < bufferSize) {
					counter++;
				} else {
					table.delete(deletes);
					deletes.clear();
					counter = 0;
				}
				
				realDelCount++;
				rec = rs.next();
			}
			
			if (deletes.size() > 0) {
				table.delete(deletes);
				deletes.clear();
			}
			
			//all successful
			
		} catch (IOException e) {
			e.printStackTrace();
			
			if ( !(e instanceof DoNotRetryIOException)) {
				
				if (e.getCause() instanceof NotServingRegionException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
				} else if (e.getCause() instanceof RegionTooBusyException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
				}
			} 			
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "delete Record error!");
			
		} finally {
			if (rs != null) {
				rs.close();  
			}
			table.close();
			
			// no match record
			if (realDelCount == 0) {
				throw new TableException(OtsErrorCode.EC_OTS_STORAGE_DELETE_RECORD_NOMATCH, "delete Record error, no match record!");
			}
		}		
	}
	
	
//	/**
//	 * 插入表数据
//	 *
//	 * @return 0-successful, other-error
//	 *
//	 * @throws IOException
//	 * @throws ZooKeeperConnectionException
//	 * @throws MasterNotRunningException
//	 * @throws TableException
//	 */
//	@SuppressWarnings("deprecation")
//	public static void insertRecords(Table table,  List<RowRecord> records) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
//
//        List<Put> lp = new ArrayList<Put>();
//        int bufferSize = OtsConstants.DEFAULT_META_SCANNER_CACHING; //avoid running out of memory
//		table.setWriteBufferSize(OtsConstants.DEFAULT_CLIENT_WRITE_BUFFER);
//
//        try {
//        	for(int i = 1; i <= records.size(); ++i) {
//        		RowRecord record = records.get(i - 1);
//        		if (record.hasRowkey()) { //important
//        			Put p = new Put((byte[])record.getRowkey());
//
//        			List<RowCell> cellList = record.getCellList();
//        			for (RowCell cell : cellList) {
//        				//if (0 != Bytes.compareTo(cell.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) { //if not key, add it as one column
//        					p.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), cell.getName(), cell.getValue());
//						//}
// 					}
//            		lp.add(p);
//            	}
//
//        		if(0 == i % bufferSize) //avoid running out of memory
//        		{
//        			table.put(lp);
//        			lp.clear();
//        		}
//        	}
//
//        	if(lp.size() > 0) {
//        		table.put(lp);
//        		lp.clear();
//        	}
//
//        	//table.flushCommits();
//
//        } catch (IOException e) {
//			e.printStackTrace();
//
//			if ( !(e instanceof DoNotRetryIOException)) {
//
//				if (e.getCause() instanceof NotServingRegionException) {
//					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
//				} else if (e.getCause() instanceof RegionTooBusyException) {
//					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
//				}
//			}
//			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_INSERT, "insert Record error!");
//
//		} finally {
//			table.close();
//		}
//	}

	/**
	 * 插入表数据
	 *
	 * @return 0-successful, other-error
	 *
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 * @throws TableException
	 */
	@SuppressWarnings("deprecation")
	public static void insertRecords(Table table, Long tableId, List<RowRecord> records) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {

			List<Put> lp = new ArrayList<Put>();
		int bufferSize = OtsConstants.DEFAULT_META_SCANNER_CACHING; //avoid running out of memory
		table.setWriteBufferSize(OtsConstants.DEFAULT_CLIENT_WRITE_BUFFER);

		try {
			for(int i = 1; i <= records.size(); ++i) {
				RowRecord record = records.get(i - 1);
				if (record.hasRowkey()) { //important

					byte[] tArRowKey = OtsAdmin.getBigRowkey(tableId,record.getRowkey());

					Put p = new Put(tArRowKey);

					List<RowCell> cellList = record.getCellList();
					for (RowCell cell : cellList) {
						//if (0 != Bytes.compareTo(cell.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) { //if not key, add it as one column
						p.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), cell.getName(), cell.getValue());
						//}
					}
					lp.add(p);
				}

				if(0 == i % bufferSize) //avoid running out of memory
				{
					table.put(lp);
					lp.clear();
				}
			}

			if(lp.size() > 0) {
				table.put(lp);
				lp.clear();
			}

			//table.flushCommits();

		} catch (IOException e) {
			e.printStackTrace();

			if ( !(e instanceof DoNotRetryIOException)) {

				if (e.getCause() instanceof NotServingRegionException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_NOSERVINGREGINON, "no serving region exception, you may retry the operation!");
				} else if (e.getCause() instanceof RegionTooBusyException) {
					throw new TableException(OtsErrorCode.EC_OTS_STORAGE_OPERATE_RECORD_REGINONTOOBUSY, "region too busy exception, you may retry the operation!");
				}
			}
			throw new TableException(OtsErrorCode.EC_OTS_STORAGE_RECORD_INSERT, "insert Record error!");

		} finally {
			table.close();
		}
	}


//	/**
//	 * 更新表数据
//	 *
//	 *
//	 * @throws IOException
//	 * @throws ZooKeeperConnectionException
//	 * @throws MasterNotRunningException
//	 * @throws TableException
//	 *
//	 */
//	public void updateRecords(Table table, List<RowRecord> records) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
//		insertRecords(table, records);
//	}
	/**
	 * 更新表数据
	 *
	 *
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 * @throws TableException
	 *
	 */
	public void updateRecords(Table table, Long tableId, List<RowRecord> records) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException {
		insertRecords(table,tableId, records);
	}
	
	/**
	 * 获取表全名
	 * 
	 * @param tenantid
	 * @param tablename
	 * @return
	 */
	public String getTableFullname(String tenantid, String tablename) {
		return (tenantid + TableName.NAMESPACE_DELIM + tablename);
	}
}
