package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="cell")
@XmlAccessorType(XmlAccessType.FIELD)
public class CellModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	/**
	 * xml/json元素名称同变量名
	 */
	@JsonProperty("column")
	@XmlElement(name="column")
	private String name;
	
	@JsonProperty("value")
	private Object value;
	
	@JsonIgnore
	private Long timestamp;
	
	/**
	 * Default constructor
	 */
	public CellModel() {}
	
	public CellModel(String name, Object value) {
		this(name, null, value);
	}
	
	public CellModel(String name, Long timestamp, Object value) {
		super();
		this.name = name;
		this.timestamp = timestamp;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute
	public Object getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@JsonIgnore
	@XmlTransient
	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
