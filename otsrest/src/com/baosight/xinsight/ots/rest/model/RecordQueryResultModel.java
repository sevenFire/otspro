package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RecordQueryResultModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonIgnore
	//@JsonProperty(value="count")
	private Long count;
	
	@JsonIgnore
	//@JsonProperty(value="table_name")
	private String tableName;
	
	@JsonProperty(value="range_key_next_cursor_mark")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private String rangekey_next_cursormark;

	//need check "hash_key","range_key" 
	@JsonProperty(value="records")
	private List<Map<String, Object> > listRecords = new ArrayList<Map<String, Object> >();
	
	/**
	 * Default constructor
	 */
	public RecordQueryResultModel() {}
	
	public RecordQueryResultModel(String name) {
		super();
		this.tableName = name;
		this.count = 0L;
		this.listRecords.clear();
	}
	
	public RecordQueryResultModel(Long totalCount, String tableName,
			List<Map<String, Object> > listRecords) {
		super();
		this.count = totalCount;
		this.tableName = tableName;
		this.listRecords = listRecords;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	@XmlElement
	public List<Map<String, Object> > getListRecords() {
		return listRecords;
	}

	public void setListRecords(List<Map<String, Object> > listRecords) {
		this.listRecords = listRecords;
	}
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}
	
	@XmlElement
	public String getRangekey_next_cursormark() {
		return rangekey_next_cursormark;
	}
	
	public void setRangekey_next_cursormark(String rangekey_next_cursormark) {
		this.rangekey_next_cursormark = rangekey_next_cursormark;
	}

	@JsonIgnore
	@XmlTransient
	public void add(Map<String, Object> r) {
		listRecords.add(r);
	}
	
	@JsonIgnore
	@XmlTransient
	public int size() {
		return listRecords.size();
	}
	
	@JsonIgnore
	@XmlTransient
	public Map<String, Object> getRec(int index) {
		return listRecords.get(index);
	}

	@JsonIgnore
	@XmlTransient
	public void clear() {
		listRecords.clear();
	}	
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
