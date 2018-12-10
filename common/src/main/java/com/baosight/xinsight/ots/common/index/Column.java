package com.baosight.xinsight.ots.common.index;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;


@XmlRootElement(name="column")
@XmlAccessorType(XmlAccessType.FIELD)
public class Column implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonProperty(value="column")
	private String name;
	
	@JsonProperty(value="type")	
	private String type;
		
	@JsonIgnore
	private Boolean indexed;
	
	@JsonIgnore
	private Boolean stored;
	
	/**
	 * Default constructor
	 */
	public Column() {}
	
	public Column(String name) {
		this(name, null);
	}
	
	public Column(String name, String type) {
		this(name, type, true, false);//default indexed=true and stored=false
	}
	
	private Column(String name, String type, Boolean indexed, Boolean stored) {
		super();
		this.name = name;
		this.type = type;
		this.indexed = indexed;
		this.stored = stored;
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
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
