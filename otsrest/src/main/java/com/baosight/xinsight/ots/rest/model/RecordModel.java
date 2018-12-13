package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="record")
@XmlAccessorType(XmlAccessType.FIELD)
public class RecordModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
		
	//need check "hash_key","range_key"  
	@JsonProperty("record")	
	private Map<String, Object> cellList = new HashMap<String, Object>();
	
	/**
	 * Default constructor
	 */
	public RecordModel() {}	
	
	public RecordModel(Map<String, Object> cellList) {
		super();
		this.cellList = cellList;
	}

	@XmlElement
	public Map<String, Object> getCellList() {
		return cellList;
	}

	public void setCellList(Map<String, Object> cellList) {
		this.cellList = cellList;
	}

	@JsonIgnore
	@XmlTransient
	public void addCell(CellModel c) {
		if (c != null) {
			cellList.put(c.getName(), c.getValue());
		}
	}

	@JsonIgnore
	@XmlTransient
	public void clear() {
		cellList.clear();
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
