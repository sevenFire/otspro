package com.baosight.xinsight.ots.client.metacfg;

public class TableProfile {
	private long id;
	private long tid;
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
	public long getTid() {
		return tid;
	}
	public void setTid(long tid) {
		this.tid = tid;
	}
}
