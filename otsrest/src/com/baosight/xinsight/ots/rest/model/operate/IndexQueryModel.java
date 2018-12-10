package com.baosight.xinsight.ots.rest.model.operate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.baosight.xinsight.common.CommonConstants;
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
public class IndexQueryModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	private String query;
	private String filters;
	private String columns;
	private String orders;
	private Long limit;
	private Long offset;
	private String cursor_mark;

	public IndexQueryModel() {}
	
	public IndexQueryModel(String query) {		
		new IndexQueryModel(query, null, null, null, null, null, null);		
	}
	
	public IndexQueryModel(String query, String filters, String columns, String orders, Long limit, Long offset, String cursor_mark) {
		super();
		this.query = query;
		this.filters = filters;
		this.columns = columns;
		this.orders = orders;
		this.limit = limit;
		this.offset = offset;
		this.cursor_mark = cursor_mark;
	}

	/**
	 *判断query是否为空
	 * 
	 * @since 2015-01-12
	 */
	@JsonIgnore
	@XmlTransient
	public boolean hasQuery() {
		return query==null?false:true;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasFilters() {
		if (filters == null || filters.isEmpty()) {
			return false;
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
	public boolean hasOrders() {
		if (orders == null || orders.isEmpty()) {
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
	
	
	/**
	 * filters格式:"filter1,filter2,filter3"
	 * filter1格式：column1:some_string
	 */
	@JsonIgnore
	@XmlTransient
	public List<String> getFiltersAsList() {
		List<String> list = new ArrayList<String>();
		if (filters != null) {
			String[] array = filters.split(CommonConstants.DEFAULT_COMMA_SPLIT);
			for (String filter : array) {
				list.add(filter);
			}
		}		
		return list;
	}
	
	/**
	 * columns格式:"column1,column2,column3"
	 */
	@JsonIgnore
	@XmlTransient
	public List<byte[]> getColumnsAsList() {
		List<byte[]> list = new ArrayList<byte[]>();
		if (columns != null) {
			String[] array = columns.split(CommonConstants.DEFAULT_COMMA_SPLIT);
			for (String column : array) {
				list.add(Bytes.toBytes(column));
			}
		}		
		return list;
	}

	/**
	 * orders格式:"column1:desc,column2:asc,column3:asc"
	 */
	@JsonIgnore
	@XmlTransient
	public List<String> getOrdersAsList() {
		List<String> list = new ArrayList<String>();
		if (orders != null) {
			String[] array = orders.split(CommonConstants.DEFAULT_COMMA_SPLIT);
			for (String order : array) {
				list.add(order);
			}
		}		
		return list;
	}
	
	//默认100，最大10000
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

	public Long getOffset() {
		return offset==null?RestConstants.DEFAULT_QUERY_OFFSET:offset.longValue();
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public String getFilters() {
		return filters;
	}

	public void setFilters(String filters) {
		this.filters = filters;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getOrders() {
		return orders;
	}

	public void setOrders(String orders) {
		this.orders = orders;
	}
	
	public void setCursor_mark(String cursor_mark) {
		this.cursor_mark = cursor_mark;
	}
	
	public String getCursor_mark() {
		return cursor_mark;
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
