package com.baosight.xinsight.ots.client.metacfg;

public class IndexProfile {
	private long id;
	private long indexid;
	private String displayCol;
	
	public String getDisplayCol() {
		return displayCol;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public void setDisplayCol(String displayCol) {
		this.displayCol = displayCol;
	}
	public long getIndexid() {
		return indexid;
	}
	public void setIndexid(long indexid) {
		this.indexid = indexid;
	}
}
