package com.baosight.xinsight.ots.common.secondaryindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn.ValueType;
import com.baosight.xinsight.ots.exception.OtsException;

public class SecondaryIndexInfo {
	private String name;

	private List<SecondaryIndexColumn> columns = new ArrayList<SecondaryIndexColumn>();
	
	private void checkColumn(String colName) throws OtsException {
		if(colName.contains("#") || colName.contains("->") 
				|| colName.contains("=>") || colName.contains("&")
				|| colName.contains(","))
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_COLUMN_NAME, "Invalid column name");
	}
	
	public SecondaryIndexInfo(String indexName)	{
		name = indexName;
	}
	
	public void addColumn(String colName) throws OtsException {
		checkColumn(colName);
		if(existColumn(colName))
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_ALREADY_EXIST, "Fail to add column, because column " + colName +  " already exist!");
		columns.add(new SecondaryIndexColumn(colName, ValueType.string, OtsConstants.OTS_SEC_INDEX_DEF_COL_MAX_LEN));
	}
	
	public void addColumn(String colName, ValueType type) throws OtsException {
		checkColumn(colName);
		if(existColumn(colName))
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_ALREADY_EXIST,
					"Fail to add column, because column " + colName +  " already exist!");
		columns.add(new SecondaryIndexColumn(colName, type));
	}
	
	public void addColumn(String colName, ValueType type, Integer maxLen) throws OtsException, IOException {
		checkColumn(colName);
		if(existColumn(colName))
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_ALREADY_EXIST, "Fail to add column, because column " + colName +  " already exist!");
		columns.add(new SecondaryIndexColumn(colName, type, maxLen));
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<SecondaryIndexColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<SecondaryIndexColumn> columns) {
		this.columns = columns;
	}
	
	public int getKeyLength() throws OtsException {
		int len = 0;
		for(SecondaryIndexColumn column:columns) {
			len += column.getMaxLen();
		}
		
		return len;
	}
	
	public int getColumnKeyOffset(String name) throws OtsException {
		int offset = 0;
		for(SecondaryIndexColumn column:columns) {
			if(name.equals(column.getName())) {
				break;
			}
			offset += column.getMaxLen();
		}
		
		return offset;		
	}
	
	public ValueType getColumnType(String name) {
		for(SecondaryIndexColumn column:columns) {
			if(name.equals(column.getName())) {
				return column.getType();
			}
		}	
		return null;
	}
	
	public boolean existColumn(String name) {
		for(SecondaryIndexColumn column:columns) {
			if(column.getName().equals(name))
				return true;
		}	
		
		return false;
	}
	
	public SecondaryIndexColumn getColumn(int i) {
		return columns.get(i);
	}

	public boolean checkColumnsDuplicateAndEmpty() {
		Map<String, ValueType> realColumnsMap = new HashMap<String, ValueType>();
		List<SecondaryIndexColumn> listOrig = getColumns();
		for (SecondaryIndexColumn col : listOrig) { 
			if (col.getName().trim().isEmpty()) {
				return true;
			}
			realColumnsMap.put(col.getName(), col.getType());
		}
		
		return !(realColumnsMap.size() == listOrig.size());
	}
}
