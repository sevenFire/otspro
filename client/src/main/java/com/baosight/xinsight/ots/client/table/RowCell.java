package com.baosight.xinsight.ots.client.table;

import java.io.Serializable;

public class RowCell implements Serializable {

	private static final long serialVersionUID = 1L;

	private byte[] name;
	
	private byte[] value;	//适配各种类型,important
	
	private Long timestamp;
	
	/**
	 * Default constructor
	 */
	public RowCell() {}
	
	public RowCell(byte[] name, byte[] value) {
		this(name, null, value);
	}
	
	public RowCell(byte[] name, Long timestamp, byte[] value) {
		super();
		this.name = name;
		this.timestamp = timestamp;
		this.value = value;
	}
	
	public byte[] getName() {
		return name;
	}

	public void setName(byte[] name) {
		this.name = name;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}





	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}
