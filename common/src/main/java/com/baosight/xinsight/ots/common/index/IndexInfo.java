package com.baosight.xinsight.ots.common.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//对外class，列均使用int32等
public class IndexInfo {
	private String name;
	private String startKey;
	private String endKey;
	private List<Column> columns;
	private char pattern;
	private Integer shardNum;
	private Integer replicationNum;
	private Integer maxShardNumPerNode;
	
	public Integer getShardNum() {
		return shardNum;
	}
	
	public void setShardNum(Integer shardNum) {
		this.shardNum = shardNum;
	}
	
	public String getStartKey() {
		return startKey;
	}
	
	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}
	
	public String getEndKey() {
		return endKey;
	}
	
	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}
	
	public List<Column> getColumns() {
		return columns;
	}
	
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	
	public char getPattern() {
		return pattern;
	}
	
	public void setPattern(char pattern) {
		this.pattern = pattern;
	}
	
	public Integer getReplicationNum() {
		return replicationNum;
	}
	
	public void setReplicationNum(Integer replicationNum) {
		this.replicationNum = replicationNum;
	}
	
	public Integer getMaxShardNumPerNode() {
		return maxShardNumPerNode;
	}
	
	public void setMaxShardNumPerNode(Integer maxShardNumPerNode) {
		this.maxShardNumPerNode = maxShardNumPerNode;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public boolean checkColumnsDuplicateAndEmpty() {
		Map<String, String> realColumnsMap = new HashMap<String, String>();
		List<Column> listOrig = getColumns();
		for (Column col : listOrig) { 
			if (col.getName().trim().isEmpty()) {
				return true;
			}
			realColumnsMap.put(col.getName(), col.getType());
		}
		
		return !(realColumnsMap.size() == listOrig.size());
	}
}
