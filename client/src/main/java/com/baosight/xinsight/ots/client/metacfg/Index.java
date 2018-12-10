package com.baosight.xinsight.ots.client.metacfg;

import java.util.Date;

public class Index {
	private long uid;
	private long tid;
	private String name;
	private String startKey;
	private String endKey;
	private char pattern;
	private String indexColumns;
	private long id;
	private int shardNum;
	private int replicationNum;
	
	private Date createTime;
	private Date lastModify;
	
	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	public long getTid() {
		return tid;
	}
	public void setTid(long tid) {
		this.tid = tid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
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
	public char getPattern() {
		return pattern;
	}
	public void setPattern(char pattern) {
		this.pattern = pattern;
	}
	public String getIndexColumns() {
		return indexColumns;
	}
	public void setIndexColumns(String indexColumns) {
		this.indexColumns = indexColumns;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public Date getLastModify() {
		return lastModify;
	}
	public void setLastModify(Date lastModify) {
		this.lastModify = lastModify;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public int getShardNum() {
		return shardNum;
	}
	public void setShardNum(int shardNum) {
		this.shardNum = shardNum;
	}
	public int getReplicationNum() {
		return replicationNum;
	}
	public void setReplicationNum(int replicationNum) {
		this.replicationNum = replicationNum;
	}
}
