package com.baosight.xinsight.ots.rest.model.operate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
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
public class RecordQueryModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="hash_key")
	private String hashkey;
	@JsonProperty(value="range_key")
	private String rangekey;
	@JsonProperty(value="range_key_prefix")
	private String rangekey_prefix;
	@JsonProperty(value="range_key_start")
	private String range_key_start;
	@JsonProperty(value="range_key_end")
	private String range_key_end;
	@JsonProperty(value="keys")
	private List<KeyModel> _full_keys;
	@JsonProperty(value="start_time")
	private Long start_time;
	@JsonProperty(value="end_time")
	private Long end_time;
	
	private Integer query_from;
	private String columns;
	@JsonProperty(value="range_key_cursor_mark")
	private String cursor_mark;
	private Long limit;
	private Long offset;
	private Boolean descending;
	
	@JsonIgnore
	private Boolean isSort;

	public RecordQueryModel() {}
	
	public RecordQueryModel(String hashkey, String rangekey, String rangekey_prefix, String range_key_start, String range_key_end, Long start_time, Long end_time) {		
		this(hashkey, rangekey, rangekey_prefix, range_key_start, range_key_end, start_time, end_time, null, null, null, null, null, null, null);		
	}
	
	public RecordQueryModel(String hashkey, String rangekey, String rangekey_prefix, String range_key_start, String range_key_end, Long start_time, Long end_time, 
			String columns, Long limit, Long offset, String cursor_mark, Boolean isSort, Boolean descending, Integer query_from) {		
		super();
		this.hashkey = hashkey;
		this.rangekey = rangekey;
		this.rangekey_prefix = rangekey_prefix;
		this.range_key_start = range_key_start;
		this.range_key_end = range_key_end;
		this.cursor_mark = cursor_mark;
		this.columns = columns;
		this.limit = limit;
		this.offset = offset;
		this.descending = descending;
		this.isSort = isSort;
		this.start_time = start_time;
		this.end_time = end_time;
		this.query_from = query_from;
	}
		
	/**
	 * 优先判断keys
	 * @param keytype 
	 * 
	 * @since 2015-01-12
	 */
	@JsonIgnore
	@XmlTransient
	public int whichRange(int keytype) {
		//for delete
		if (end_time != null) {
			return RestConstants.TIMERANGE_OPERATE;
		} 
		
		//keys first
		if (_full_keys != null) {
			return RestConstants.MULTIKEY_OPERATE;
		}
		
		if (hashkey == null) {
			return RestConstants.ANYKEY_OPERATE;
		} 
			
		if (keytype == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
			return RestConstants.KEY_OPERATE;
		}
		else if (keytype == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
			if (rangekey != null) {			
				return RestConstants.KEY_OPERATE;
			}
			
			if (rangekey_prefix != null) {
				if (range_key_start == null && range_key_end == null) {
					return RestConstants.PREFIX_OPERATE;
				} 
							
				return RestConstants.RANGE_PREFIX_OPERATE;
			} else {			
				if (range_key_start != null || range_key_end != null) {
					return RestConstants.RANGE_OPERATE;
				} 			
				
				return RestConstants.HASH_OPERATE;
			}
		}
		
		return RestConstants.UNKNOWN_OPERATE;
	}
	
	/**
	 * 
	 *  @param keytype 
	 * @since 2015-01-12
	 * 
	 */
	@JsonIgnore
	@XmlTransient
	public boolean isValidQuery(int keytype) {		
		if (whichRange(keytype) == RestConstants.UNKNOWN_OPERATE) {			
			return false;
		}
		
		if (hashkey != null) {
			if (hashkey.isEmpty()) {
				return false;
			}
		}
			
		if (limit != null) {
			if (limit < 0) {
				return false;
			}
		}
		
		if (cursor_mark == null) {
			if (offset != null) {
				if (offset < 0) {
					return false;
				}
			}
		}
		
		return true;
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
	public int whichIterate() {
		if (cursor_mark != null) {
			return RestConstants.ITERATE_CURSOR_MARK;
		} 
		
		return RestConstants.ITERATE_OFFSET;
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
		
	@XmlElement
	public String getHashkey() {
		return hashkey;
	}

	public void setHashkey(String hashkey) {
		this.hashkey = hashkey;
	}
	
	@XmlElement
	public String getRangekey() {
		return rangekey;
	}

	public void setRangekey(String rangekey) {
		this.rangekey = rangekey;
	}

	@XmlElement
	public String getRangekey_prefix() {
		return rangekey_prefix;
	}

	public void setRangekey_prefix(String rangekey_exp) {
		this.rangekey_prefix = rangekey_exp;
	}

	@XmlElement
	public String getRange_key_start() {
		return range_key_start;
	}

	public void setRange_key_start(String range_key_start) {
		this.range_key_start = range_key_start;
	}

	@XmlElement
	public String getRange_key_end() {
		return range_key_end;
	}

	public void setRange_key_end(String range_key_end) {
		this.range_key_end = range_key_end;
	}
	
	/**
	 * 是否降序排列,默认降序
	 * 
	 * @since 2015-01-12
	 */
	@JsonIgnore
	@XmlTransient
	public boolean isDescending() {
		if (descending == null) {
			return false;
		}
		
		return descending.booleanValue();
	}
	
	/**
	 * columns格式:"col1,col2,col3"
	 * @return 列名数组
	 */
	@JsonIgnore
	@XmlTransient
	public String[] getColumnsArray() {		
		if (columns != null) {
			return columns.split(CommonConstants.DEFAULT_COMMA_SPLIT);
		}
		
		return null;
	}
	
	@JsonIgnore
	@XmlTransient
	public List<byte[]> getColumnsAsList() {	
		List<byte[]> list = new ArrayList<byte[]>();
		if (columns != null) {
			String[] array = columns.split(CommonConstants.DEFAULT_COMMA_SPLIT);
			for (String col : array) {
				list.add(Bytes.toBytes(col));
			}
		}
		
		return list;
	}
	
	@XmlElement
	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	@XmlElement
	public Long getLimit() {
		//return limit;
		if (limit==null) {
			return RestConstants.DEFAULT_QUERY_LIMIT;
		} else {
			if (limit.longValue() >= RestConstants.DEFAULT_QUERY_MAX_LIMIT) {
				return RestConstants.DEFAULT_QUERY_MAX_LIMIT;
			} else {
				return limit.longValue();
			}
		}
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	@XmlElement
	public Long getOffset() {
		return offset==null?RestConstants.DEFAULT_QUERY_OFFSET:offset.longValue();
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	@XmlElement
	public Boolean getDescending() {
		return descending;
	}	

	@XmlElement
	public Integer getQuery_from() {
		return query_from==null?RestConstants.QUERY_FROM_TYPE_REST:query_from;
	}

	public void setQuery_from(Integer query_from) {
		this.query_from = query_from;
	}

	public void setDescending(Boolean descending) {
		this.descending = descending;
	}
	
	@XmlElement
	public String getCursor_mark() {
		return cursor_mark;
	}

	public void setCursor_mark(String cursor_mark) {
		this.cursor_mark = cursor_mark;
	}
			
	@XmlElement
	public List<KeyModel> get_full_keys() {
		return _full_keys;
	}

	public void set_full_keys(List<KeyModel> _full_keys) {
		this._full_keys = _full_keys;
	}

	@JsonIgnore
	@XmlTransient
	public Boolean getIsSort() {
		if (isSort == null) {
			return true;
		}		
		return isSort;
	}

	public void setIsSort(Boolean isSort) {
		this.isSort = isSort;
	}
	
	@XmlElement
	public Long getStart_time() {
		return start_time;
	}

	public void setStart_time(Long start_time) {
		this.start_time = start_time;
	}

	@XmlElement
	public Long getEnd_time() {
		return end_time;
	}

	public void setEnd_time(Long end_time) {
		this.end_time = end_time;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
