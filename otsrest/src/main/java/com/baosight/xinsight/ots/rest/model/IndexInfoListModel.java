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
 * Simple representation of a list of index info.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class IndexInfoListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonIgnore
	//@JsonProperty(value="count")
	private Integer count;

	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="index_info_list")
	private List<IndexInfoModel> indexinfolist = new ArrayList<IndexInfoModel>();

	/**
	 * Default constructor
	 */
	public IndexInfoListModel() {}
	
	public IndexInfoListModel(Integer count) {
		super();
		this.count = count;
		this.indexinfolist.clear();
	}

	public IndexInfoListModel(Integer count, List<IndexInfoModel> indexinfolist) {
		super();
		this.count = count;
		this.indexinfolist = indexinfolist;
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
	 * Add the index info model to the list
	 * @param index the index model
	 */
	@JsonIgnore
	@XmlTransient
	public void add(IndexInfoModel index) {
		indexinfolist.add(index);
	}
	
	/**
	 * @param index the index
	 * @return the index model
	 */
	@JsonIgnore
	@XmlTransient
	public IndexInfoModel get(int index) {
		return indexinfolist.get(index);
	}

	/**
	 * @return the info list
	 */
	@XmlElement
	public List<IndexInfoModel> getIndexinfolist() {
		return indexinfolist;
	}

	/**
	 * @param indexinfolist the index info to set
	 */
	public void setIndexinfolist(List<IndexInfoModel> indexinfolist) {
		this.indexinfolist = indexinfolist;
	}
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}

	@Override
	public String toString() {

		return JsonUtil.toJsonString(this);
	}
}
