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
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement(name="tableinfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableInfoModel implements Serializable {
	
	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="table_name")
	private String name;
	
	@JsonProperty(value="compression_type")
	private Integer compressionType;
	
	@JsonIgnore
	//@JsonProperty(value="enabled")
	private Boolean enable;
	
	@JsonProperty(value="primary_key_type")
	private Integer keyType;	
	
	@JsonProperty(value="hash_key_type")
	private Integer hashKeyType;

	@JsonProperty(value="range_key_type")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Integer rangeKeyType;
		
	@JsonProperty(value="create_time")
	@XmlElement(name="create_time")
	private String createTime;
	
	@JsonProperty(value="modify_time")
	@XmlElement(name="modify_time")
	private String lastModify; 

	@JsonIgnore
	//@JsonProperty(value="max_versions")
	private Long maxVersions;
	
	@JsonProperty(value="mob_enabled")
	private Boolean mobEnabled;
	
	@JsonProperty(value="mob_threshold")
	private Long mobThreshold;
	
	@JsonIgnore
	//@JsonProperty(value="region_count")	
	private Long regionCount;
	
	@JsonIgnore
	//@JsonProperty(value="disk_size")
	private Long diskSize;
	
	@JsonIgnore
	//@JsonProperty(value="index_size")
	private Long indexSize;
	
	@JsonIgnore
	//@JsonProperty(value="read_count")
	private Long readCount;

	@JsonIgnore
	//@JsonProperty(value="write_count")
	private Long writeCount;  
  
	@JsonProperty(value="description")
	private String description;
	
	//@JsonIgnore
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private long id;
	
	/**
	* Default constructor
	*/
	public TableInfoModel() {}

	public TableInfoModel(String name) {
		this(name, null, null, null, null, null, null, null, null, null, null, null);
	}
	
	public TableInfoModel(String name, String description, Integer compressionType, 
			Boolean enable,	Long maxVersions, Boolean mobEnabled, Long mobThreshold) {
		super();
		this.name = name;
		this.description = description;
		this.compressionType = compressionType;
		this.enable = enable;
		this.maxVersions = maxVersions;
		this.regionCount = null;
		this.diskSize = null;
		this.indexSize = null;
		this.readCount = null;
		this.writeCount = null;
		this.mobEnabled = mobEnabled;
		this.mobThreshold = mobThreshold;
	}
	
	public TableInfoModel(String name, String description, Integer compressionType, 
			Boolean enable,	Long maxVersions, Boolean mobEnabled, Long mobThreshold , Long regionCount, Long diskSize,
			Long indexSize, Long readCount, Long writeCount) {
		super();
		this.name = name;
		this.description = description;
		this.compressionType = compressionType;
		this.enable = enable;
		this.maxVersions = maxVersions;
		this.regionCount = regionCount;
		this.diskSize = diskSize;
		this.indexSize = indexSize;
		this.readCount = readCount;
		this.writeCount = writeCount;
		this.mobEnabled = mobEnabled;
		this.mobThreshold = mobThreshold;
	}

	@XmlAttribute
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@XmlElement
	public String getDescription() {
		return description==null?"":description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@XmlElement
	public Integer getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(Integer compressionType) {
		this.compressionType = compressionType;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Boolean getEnable() {
		return enable;
	}

	public void setEnable(Boolean enable) {
		this.enable = enable;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getMaxVersions() {
		return maxVersions==null?1:maxVersions;
	}

	public void setMaxVersions(Long maxVersions) {
		this.maxVersions = maxVersions;
	}
	
	@XmlElement
	public Boolean getMobEnabled() {
		return this.mobEnabled;
	}

	public void setMobEnabled(Boolean mobEnabled) {
		this.mobEnabled = mobEnabled;
	}
	
	@XmlElement
	public Long getMobThreshold() {
		return this.mobThreshold;
	}

	public void setMobThreshold(Long mobThreshold) {
		this.mobThreshold = mobThreshold;
	}
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}

	@XmlElement
	public Integer getKeyType() {
		return keyType;
	}

	public void setKeyType(Integer keyType) {
		this.keyType = keyType;
	}

	@XmlElement
	public Integer getHashKeyType() {
		return hashKeyType;
	}

	public void setHashKeyType(Integer hashKeyType) {
		this.hashKeyType = hashKeyType;
	}

	@XmlElement
	public Integer getRangeKeyType() {
		return rangeKeyType;
	}

	public void setRangeKeyType(Integer rangeKeyType) {
		this.rangeKeyType = rangeKeyType;
	}
	
	@XmlElement
	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	@XmlElement
	public String getLastModify() {
		return lastModify;
	}

	public void setLastModify(String lastModify) {
		this.lastModify = lastModify;
	}
	
	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getRecordCount() {
		return regionCount;
	}

	public void setRecordCount(Long regionCount) {
		this.regionCount = regionCount;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(Long diskSize) {
		this.diskSize = diskSize;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getIndexSize() {
		return indexSize;
	}

	public void setIndexSize(Long indexSize) {
		this.indexSize = indexSize;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getReadCount() {
		return readCount;
	}

	public void setReadCount(Long readCount) {
		this.readCount = readCount;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Long getWriteCount() {
		return writeCount;
	}

	public void setWriteCount(Long writeCount) {
		this.writeCount = writeCount;
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
