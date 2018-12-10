package com.baosight.xinsight.ots.rest.model;

import java.io.ByteArrayInputStream;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="tableconfig")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableConfigModel {
	@JsonIgnore
	private static final long serialVersionUID = 1L;

	/**
	 * xml/json元素名称同变量名
	 */
	@JsonProperty("columns")
	@XmlElement(name="columns")
	private String columns;
	
	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	/**
	 * Default constructor
	 */
	public TableConfigModel() {}
	
	public TableConfigModel(String columns) {
		this(columns, null);
	}
	
	public TableConfigModel(String columns, Long errcode) {
		super();
		this.columns = columns;
		this.errcode = errcode;
	}
	
	@XmlAttribute
	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}	
	
	@XmlAttribute
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

	@JsonIgnore
	@XmlTransient
    public static TableConfigModel toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TableConfigModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TableConfigModel failed.");
		}		
    }
}
