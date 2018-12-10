package com.baosight.xinsight.ots.rest.model;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="records")
@XmlAccessorType(XmlAccessType.FIELD)
public class RecordListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
		
	//need check "hash_key","range_key" for each record
	@JsonProperty(value="records")
	private List<Map<String, Object> > recordList = new ArrayList<Map<String, Object> >();
	
	/**
	 * Default constructor
	 */
	public RecordListModel() {}
	
	public RecordListModel(List<Map<String, Object> > recordList) {
		super();
		this.recordList = recordList;
	}

	@XmlElement
	public List<Map<String, Object> > getRecordList() {
		return recordList;
	}

	public void setRecList(List<Map<String, Object> > recordList) {
		this.recordList = recordList;
	}

	@JsonIgnore
	@XmlTransient
	public Object getHashkey(int index) {
		return recordList.get(index).get(RestConstants.OTS_HASHKEY);
	}
	
	@JsonIgnore
	@XmlTransient
	public Object getRegionkey(int index) {
		return recordList.get(index).get(RestConstants.OTS_RANGEKEY);
	}
	
	@JsonIgnore
	@XmlTransient
	public void add(Map<String, Object> r) {
		recordList.add(r);
	}
	
	@JsonIgnore
	@XmlTransient
	public int size() {
		return recordList.size();
	}
	
	@JsonIgnore
	@XmlTransient
	public Map<String, Object> getRec(int index) {
		return recordList.get(index);
	}

	@JsonIgnore
	@XmlTransient
	public void clear() {
		recordList.clear();
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static RecordListModel toClass(String in) throws OtsException {		
		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, RecordListModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to RecordListModel failed.");
		}
    }
}
