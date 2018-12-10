package com.baosight.xinsight.ots.common.index;

import java.io.ByteArrayInputStream;
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

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.utils.JsonUtil;


@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ColumnListConvert implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="columns")
	@XmlElement(name="columns")
	private List<Column> columnList = new ArrayList<Column>();
		
	/**
	 * Default constructor
	 */
	public ColumnListConvert() {}
	
	public ColumnListConvert(List<Column> columnList) {
		super();
		this.columnList = columnList;
	}
	
	@XmlElement
	public List<Column> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}

	@JsonIgnore
	@XmlTransient
	public void addColumn(Column c) {
		if (c != null) {
			columnList.add(c);
		}
	}

	@JsonIgnore
	@XmlTransient
	public void clear() {
		columnList.clear();
	}
	
	@JsonIgnore
	@XmlTransient
	public void size() {
		columnList.size();
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static ColumnListConvert toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, ColumnListConvert.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to ColumnListModel failed.");
		}		
    }
}
