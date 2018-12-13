package com.baosight.xinsight.ots.rest.model.operate;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.utils.JsonUtil;


/**
 * 该类仅仅是url参数组合类
 * 
 * @author huangming
 *
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SecondIndexQueryModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	private String hashkey;
	private String range_key_start;
	private String range_key_end;
	private String columns;
	private String column_ranges;
	private Integer limit;
	private Integer offset;
	private String cursor_mark;

	public SecondIndexQueryModel() {}
	
	public SecondIndexQueryModel(String hashkey) {		
		new SecondIndexQueryModel(hashkey, null, null, null, null, null, null, null);		
	}
	
	public SecondIndexQueryModel(String hashkey, String range_key_start, String range_key_end, String columns, String column_ranges, Integer limit, Integer offset, String cursor_mark) {
		super();
		this.hashkey = hashkey;
		this.range_key_start = range_key_start;
		this.range_key_end = range_key_end;
		this.columns = columns;
		this.column_ranges = column_ranges;
		this.limit = limit;
		this.offset = offset;
		this.cursor_mark = cursor_mark;
	}

	@JsonIgnore
	@XmlTransient
	public boolean hasColumns() {
		if (columns == null || columns.isEmpty()) {
			return false;
		}
				
		return true;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasLimit() {
		return limit==null?false:true;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasOffset() {
		return offset==null?false:true;
	}
		
	//默认100，最大10000
	public Integer getLimit() {
		//return limit;
		if (limit==null) {
			return (int)RestConstants.DEFAULT_QUERY_LIMIT;
		} else {
			if (limit.intValue() >= RestConstants.DEFAULT_QUERY_MAX_LIMIT) {
				return (int)RestConstants.DEFAULT_QUERY_MAX_LIMIT;
			} else {
				return limit.intValue();
			}
		}
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public Integer getOffset() {
		return offset==null?(int)RestConstants.DEFAULT_QUERY_OFFSET:offset.intValue();
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}
	
	public void setCursor_mark(String cursor_mark) {
		this.cursor_mark = cursor_mark;
	}
	
	public String getCursor_mark() {
		return cursor_mark;
	}

	public String getHashkey() {
		return hashkey;
	}

	public void setHashkey(String hashkey) {
		this.hashkey = hashkey;
	}

	public String getRange_key_start() {
		return range_key_start;
	}

	public void setRange_key_start(String range_key_start) {
		this.range_key_start = range_key_start;
	}
	
	public String getRange_key_end() {
		return range_key_end;
	}

	public void setRange_key_end(String range_key_end) {
		this.range_key_end = range_key_end;
	}
	
	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}
	
	public String getColumn_ranges() {
		return column_ranges;
	}

	public void setColumn_ranges(String column_ranges) {
		this.column_ranges = column_ranges;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasCursor() {
		return cursor_mark==null?false:true;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
