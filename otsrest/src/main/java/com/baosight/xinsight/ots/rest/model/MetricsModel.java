package com.baosight.xinsight.ots.rest.model;

import java.io.ByteArrayInputStream;
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

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="metrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricsModel  implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty("table_name")
	@XmlElement(name="table_name")
	private String tablename;
	
	@JsonProperty("read_count")
	@XmlElement(name="read_count")
	private Long readRequestsCount;
	
	@JsonProperty("write_count")
	@XmlElement(name="write_count")
	private Long writeRequestsCount;
	
	@JsonProperty("disk_size")
	@XmlElement(name="disk_size")
	private Long storefileSize;
	
	@JsonIgnore	
	//@JsonProperty("record_count")
	//@XmlElement(name="record_count")
	private Long recordCount;
	
	@JsonProperty("region_count")
	@XmlElement(name="region_count")
	private Long regionCount;
	
	/**
	 * Default constructor
	 */
	public MetricsModel() {}
	
	public MetricsModel(String tablename) {
		this(tablename, null, null, null, null);
	}
	
	public MetricsModel(String tablename, Long readRequestsCount, Long writeRequestsCount, Long storefileSize, Long regionCount) {
		super();
		this.tablename = tablename;
		this.readRequestsCount = readRequestsCount;
		this.writeRequestsCount = writeRequestsCount;
		this.storefileSize = storefileSize;
		this.regionCount = regionCount;
	}

	@XmlAttribute
	public Long getReadRequestsCount() {
		return readRequestsCount;
	}

	public void setReadRequestsCount(Long readRequestsCount) {
		this.readRequestsCount = readRequestsCount;
	}

	@XmlAttribute
	public Long getWriteRequestsCount() {
		return writeRequestsCount;
	}

	public void setWriteRequestsCount(Long writeRequestsCount) {
		this.writeRequestsCount = writeRequestsCount;
	}

	@XmlAttribute
	public Long getStorefileSize() {
		return storefileSize;
	}

	public void setStorefileSize(Long storefileSize) {
		this.storefileSize = storefileSize;
	}

	@XmlAttribute
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	
	@XmlAttribute
	public Long getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(Long recordCount) {
		this.recordCount = recordCount;
	}
	
	@XmlAttribute
	public Long getRegionCount() {
		return regionCount;
	}

	public void setRegionCount(Long regionCount) {
		this.regionCount = regionCount;
	}
	
	@XmlElement
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
    public static MetricsModel toClass(String in) throws OtsException {	
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, MetricsModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to MetricsModel failed.");
		}		
    }
}
