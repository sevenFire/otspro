package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.utils.JsonUtil;


/**
 * Simple representation of a list of table info.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class TableInfoListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonIgnore
	//@JsonProperty(value="count")
	private Integer count;

	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="table_info_list")
	private List<TableInfoModel> tableinfolist = new ArrayList<TableInfoModel>();

	/**
	 * Default constructor
	 */
	public TableInfoListModel() {}
	
	public TableInfoListModel(Integer count) {
		super();
		this.count = count;
		this.tableinfolist.clear();
	}

	public TableInfoListModel(Integer count, List<TableInfoModel> tableinfolist) {
		super();
		this.count = count;
		this.tableinfolist = tableinfolist;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}
	
	/**
	 * Add the table info model to the list
	 * @param index the table model
	 */
	@JsonIgnore
	@XmlTransient
	public void add(TableInfoModel index) {
		tableinfolist.add(index);
	}
	
	/**
	 * @param index the index
	 * @return the table model
	 */
	@JsonIgnore
	@XmlTransient
	public TableInfoModel get(int index) {
		return tableinfolist.get(index);
	}

	/**
	 * @return the table info list
	 */
	@XmlElement
	public List<TableInfoModel> getTableinfolist() {
		return tableinfolist;
	}

	/**
	 * @param tableinfolist the table info to set
	 */
	public void setTableinfolist(List<TableInfoModel> tableinfolist) {
		this.tableinfolist = tableinfolist;
	}
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}

	@JsonIgnore
	@XmlTransient
	@Override
	public String toString() {
		return JsonUtil.toJsonString(this);
	}
}
