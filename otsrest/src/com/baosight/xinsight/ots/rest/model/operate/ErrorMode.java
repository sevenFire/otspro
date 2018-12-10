package com.baosight.xinsight.ots.rest.model.operate;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="error")
@XmlAccessorType(XmlAccessType.FIELD)
public class ErrorMode implements Serializable  {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="errcode")
	private Long error_code;
	
	@JsonProperty(value="errinfo")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private String errinfo;
	
	
	/**
	* Default constructor
	*/
	public ErrorMode() {}

	public ErrorMode(Long error_code) {
		super();
		this.error_code = error_code;
	}
	
	public ErrorMode(Long error_code, String errinfo) {
		super();
		this.error_code = error_code;
		this.errinfo = errinfo;
	}

	@XmlAttribute
	public Long getError_code() {
		return error_code;
	}

	public void setError_code(Long error_code) {
		this.error_code = error_code;
	}	
	
	@XmlElement
	public String getErrinfo() {
		return errinfo;
	}

	public void setErrinfo(String errinfo) {
		this.errinfo = errinfo;
	}	
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
