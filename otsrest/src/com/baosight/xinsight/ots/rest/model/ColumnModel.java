package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="column")
@XmlAccessorType(XmlAccessType.FIELD)
public class ColumnModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	/**
	 * xml/json元素名称同变量名
	 */
	@JsonProperty(value="column")
	private String name;
	
	@JsonProperty(value="type")	
	private String type;
		
	//@JsonProperty(value="indexed")
	@JsonIgnore
	private Boolean indexed;
	
	//@JsonProperty(value="stored")	
	@JsonIgnore
	private Boolean stored;
	
	@JsonProperty(value="maxLen")	
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Integer maxLen;

	/**
	 * Default constructor
	 */
	public ColumnModel() {}
	
	public ColumnModel(String name) {
		this(name, null);
	}
	
	public ColumnModel(String name, String type) {
		this(name, type, true, false, null);//default indexed=true and stored=false
	}
	
	public ColumnModel(String name, String type, Integer maxLen) {
		this(name, type, false, false, maxLen);//default indexed=true and stored=false
	}
	
	private ColumnModel(String name, String type, Boolean indexed, Boolean stored, Integer maxLen) {
		super();
		this.name = name;
		this.type = type;
		this.indexed = indexed;
		this.stored = stored;
		this.maxLen = maxLen;
	}

	@XmlAttribute
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	//@XmlAttribute
	@JsonIgnore
	@XmlTransient
	public Boolean getIndexed() {
		return true;
	}

	
	public void setIndexed(Boolean indexed) {
		this.indexed = indexed;
	}

	//@XmlAttribute
	@JsonIgnore
	@XmlTransient
	public Boolean getStored() {
		return false;
	}

	@JsonIgnore
	@XmlTransient
	public void setStored(Boolean stored) {
		this.stored = stored;
	}	

	
	public Integer getMaxLen() {
		return maxLen;
	}

	public void setMaxLen(Integer maxLen) {
		this.maxLen = maxLen;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
