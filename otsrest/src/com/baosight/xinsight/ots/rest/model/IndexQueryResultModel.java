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
public class IndexQueryResultModel implements Serializable{

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="match_count")
	private Long matchCount;

	@JsonIgnore
	//@JsonProperty(value="count")
	private Long count;
	
	@JsonIgnore
	//@JsonProperty(value="table_name")
	private String tableName;
	
	@JsonProperty(value="records")
	private List<Map<String, Object> > listRecords = new ArrayList<Map<String, Object> >();
	
	@JsonProperty(value="next_cursor_mark")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private String nextCursorMark;

	/**
	 * Default constructor
	 */
	public IndexQueryResultModel() {}
	
	public IndexQueryResultModel(String name) {
		super();
		this.tableName = name;
		this.count = (long) 0;
		this.matchCount = (long) 0;
		this.listRecords.clear();
	}
	
	public IndexQueryResultModel(Long count, Long matchCount, String tableName,
			List<Map<String, Object> > listRecords) {
		super();
		this.matchCount = matchCount;
		this.count = count;
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

	@XmlElement	
	public Long getMatchCount() {
		return matchCount;
	}

	public void setMatchCount(Long matchCount) {
		this.matchCount = matchCount;
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
	public String getNextCursorMark() {
		return nextCursorMark;
	}

	public void setNextCursorMark(String nextCursorMark) {
		this.nextCursorMark = nextCursorMark;
	}	
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}
	
	@XmlElement
	public List<Map<String, Object> > getListRecords() {
		return listRecords;
	}

	public void setListRecords(List<Map<String, Object> > listRecords) {
		this.listRecords = listRecords;
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
