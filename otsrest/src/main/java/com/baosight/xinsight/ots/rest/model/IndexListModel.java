package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;


/**
 * Simple representation of a list of index names.
 */
@XmlRootElement(name="indexes")
@XmlAccessorType(XmlAccessType.FIELD)
public class IndexListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonProperty(value="errcode")
	private Long errcode;
	
	@JsonIgnore
	//@JsonProperty(value="count")
	private Integer count;

	@JsonProperty(value="index_names")
	private List<String> indexes = new ArrayList<String>();

	/**
	 * Default constructor
	 */
	public IndexListModel() {}
	
	public IndexListModel(Integer count) {
		super();
		this.count = count;
		this.indexes.clear();
	}

	public IndexListModel(Integer count, List<String> indexes) {
		super();
		this.count = count;
		this.indexes = indexes;
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
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}
	
	/**
	 * Add the index name model to the list
	 * @param index the index model
	 */
	@JsonIgnore
	@XmlTransient
	public void add(String index) {
		indexes.add(index);
	}
	
	/**
	 * @param index the index
	 * @return the index model
	 */
	@JsonIgnore
	@XmlTransient
	public String get(int index) {
		return indexes.get(index);
	}

	/**
	 * @return the tables
	 */
	@XmlElement
	public List<String> getIndexes() {
		return indexes;
	}

	/**
	 * @param indexes the indexes to set
	 */
	public void setIndexes(List<String> indexes) {
		this.indexes = indexes;
	}

	@Override
	public String toString() {
		return JsonUtil.toJsonString(this);
	}
}
