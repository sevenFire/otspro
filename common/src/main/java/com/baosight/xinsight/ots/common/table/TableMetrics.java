package com.baosight.xinsight.ots.common.table;

import java.io.Serializable;

public class TableMetrics implements Serializable {

	private static final long serialVersionUID = 1L;

	private String tablename;
	
	private Long lReadRequestsCount;
	
	private Long lWriteRequestsCount;
	
	private Long lStorefileSize;
	
	private Long lRegionCount;
		
	/**
	 * Default constructor
	 */
	public TableMetrics() {}
	
	public TableMetrics(String tablename) {
		this(tablename, null, null, null, null);
	}
	
	public TableMetrics(String tablename, Long lReadRequestsCount, Long lWriteRequestsCount, Long lStorefileSize, Long lRegionCount) {
		super();
		this.tablename = tablename;
		this.lReadRequestsCount = lReadRequestsCount;
		this.lWriteRequestsCount = lWriteRequestsCount;
		this.lStorefileSize = lStorefileSize;
		this.lRegionCount = lRegionCount;
	}

	public Long getlReadRequestsCount() {
		return lReadRequestsCount;
	}

	public void setlReadRequestsCount(Long lReadRequestsCount) {
		this.lReadRequestsCount = lReadRequestsCount;
	}

	public Long getlWriteRequestsCount() {
		return lWriteRequestsCount;
	}

	public void setlWriteRequestsCount(Long lWriteRequestsCount) {
		this.lWriteRequestsCount = lWriteRequestsCount;
	}

	public Long getlStorefileSize() {
		return lStorefileSize;
	}

	public void setlStorefileSize(Long lStorefileSize) {
		this.lStorefileSize = lStorefileSize;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public Long getlRegionCount() {
		return lRegionCount;
	}

	public void setlRegionCount(Long lRegionCount) {
		this.lRegionCount = lRegionCount;
	}
	
}
