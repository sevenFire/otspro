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
 * Simple representation of a list of metrics info.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricsListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonIgnore
	//@JsonProperty(value="count")
	private Integer count;

	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="metric_info_list")
	private List<MetricsModel> metricsinfolist = new ArrayList<MetricsModel>();

	/**
	 * Default constructor
	 */
	public MetricsListModel() {}
	
	public MetricsListModel(Integer count) {
		super();
		this.count = count;
		this.metricsinfolist.clear();
	}

	public MetricsListModel(Integer count, List<MetricsModel> metricsinfolist) {
		super();
		this.count = count;
		this.metricsinfolist = metricsinfolist;
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
	 * Add the metrics info model to the list
	 * @param index the metrics model
	 */
	@JsonIgnore
	@XmlTransient
	public void add(MetricsModel index) {
		metricsinfolist.add(index);
	}
	
	/**
	 * @param index the index
	 * @return the metrics model
	 */
	@JsonIgnore
	@XmlTransient
	public MetricsModel get(int index) {
		return metricsinfolist.get(index);
	}

	/**
	 * @return the metrics info list
	 */
	@XmlElement
	public List<MetricsModel> getMetricsinfolist() {
		return metricsinfolist;
	}

	/**
	 * @param metricsinfolist the metrics info to set
	 */
	public void setMetricsinfolist(List<MetricsModel> metricsinfolist) {
		this.metricsinfolist = metricsinfolist;
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
