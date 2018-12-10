package com.baosight.xinsight.ots.client.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RecordResult implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Long count;
	
	private String next_rowkey;//base64
		
	private List<RowRecord> listRecords = new ArrayList<RowRecord>();
	
	/**
	 * Default constructor
	 */	
	public RecordResult() {
		super();
		this.count = 0L;
		this.next_rowkey = null;
		this.listRecords.clear();
	}
	
	public RecordResult(Long count, List<RowRecord> listRecords) {
		super();
		this.count = count;
		this.next_rowkey = null;
		this.listRecords = listRecords;
	}
	
	public RecordResult(Long count, String next_rowkey, List<RowRecord> listRecords) {
		super();
		this.count = count;
		this.next_rowkey = next_rowkey;
		this.listRecords = listRecords;
	}
	
	public List<RowRecord> getListRecords() {
		return listRecords;
	}

	public void setListRecords(List<RowRecord> listRecords) {
		this.listRecords = listRecords;
	}
	
	public void add(RowRecord r) {
		listRecords.add(r);
	}
	
	public int size() {
		return listRecords.size();
	}
	
	public RowRecord getRec(int index) {
		return listRecords.get(index);
	}

	public void clear() {
		listRecords.clear();
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String getNext_rowkey() {
		return next_rowkey;
	}

	public void setNext_rowkey(String next_rowkey) {
		this.next_rowkey = next_rowkey;
	}

//	@Override
//	public  String toString(){
//		for(int i = 0;i < listRecords.size();i++){
//			System.out.println("3_lists:" + listRecords.get(i));
//		}
//		return null;
//	}
}
