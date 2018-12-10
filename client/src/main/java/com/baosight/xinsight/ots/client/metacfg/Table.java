package com.baosight.xinsight.ots.client.metacfg;

import java.util.Date;

public class Table {
	private long id;
	private long uid;
	private String name;
	private String compression;
	private String desp;
	private int enable;
	private long maxversion;
	private long tid;
	private long mobThreshold;
	private int mobEnabled;
	
	private int keytype;
	private int hashkeyType;
	private int rangekeyType;
	private Date createTime;
	private Date modifyTime;
	private long modifyUid;
	
	public int getKeytype() {
		return keytype;
	}
	public void setKeytype(int keytype) {
		this.keytype = keytype;
	}
	public int getHashkeyType() {
		return hashkeyType;
	}
	public void setHashkeyType(int hashkeyType) {
		this.hashkeyType = hashkeyType;
	}
	public int getRangekeyType() {
		return rangekeyType;
	}
	public void setRangekeyType(int rangekeyType) {
		this.rangekeyType = rangekeyType;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public Date getModifyTime() {
		return modifyTime;
	}
	public void setModifyTime(Date modifyTime) {
		this.modifyTime = modifyTime;
	}
	public long getModifyUid() {
		return modifyUid;
	}
	public void setModifyUid(long modifyUid) {
		this.modifyUid = modifyUid;
	}
	
	public long getMobThreshold() {
		return mobThreshold;
	}
	public void setMobThreshold(long mobThreshold) {
		this.mobThreshold = mobThreshold;
	}
	public int getMobEnabled() {
		return mobEnabled;
	}
	public void setMobEnabled(int mobEnabled) {
		this.mobEnabled = mobEnabled;
	}
	
	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCompression() {
		return compression;
	}
	public void setCompression(String compression) {
		this.compression = compression;
	}
	public String getDesp() {
		return desp;
	}
	public void setDesp(String desp) {
		this.desp = desp;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public int getEnable() {
		return enable;
	}
	public void setEnable(int enable) {
		this.enable = enable;
	}	
	public long getMaxversion() {
		return maxversion;
	}
	public void setMaxversion(long maxversion) {
		this.maxversion = maxversion;
	}
	public long getTid() {
		return tid;
	}
	public void setTid(long tid) {
		this.tid = tid;
	}
}
