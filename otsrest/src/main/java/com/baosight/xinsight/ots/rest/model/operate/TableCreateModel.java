package com.baosight.xinsight.ots.rest.model.operate;

import java.io.ByteArrayInputStream;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.service.TableService;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TableCreateModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="primary_key_type")
	private Integer keyType;
		
	@JsonProperty(value="hash_key_type")
	private Integer hashKeyType;
	
	@JsonProperty(value="range_key_type")
	private Integer rangeKeyType;//optional, decided by keyType
	
	@JsonProperty(value="compression_type")
	private Integer compressionType;	//optional
	
	@JsonIgnore
	//@JsonProperty(value="max_versions")	
	private Integer maxVersions;	//optional
	
	@JsonProperty(value="replication")	
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Integer replication;	//optional
	
	@JsonProperty(value="description")
	private String description;	//optional
	
	@JsonProperty(value="mob_enabled")
	private Boolean mobEnabled;	//optional
	
	@JsonProperty(value="mob_threshold")
	private Integer mobThreshold;	//optional
	
	TableCreateModel() {}
	
	public TableCreateModel(Integer compressionType) {
		this(compressionType, null, null);
	}
		
	public TableCreateModel(Integer compressionType, Integer maxVersions, String description) {
		this(compressionType, maxVersions, null, null, description);
	}
	
	public TableCreateModel(Integer compressionType, Integer maxVersions, Boolean mobEnabled, Integer mobThreshold, String description) {
		super();
		this.compressionType = compressionType;
		this.maxVersions = maxVersions;
		this.description = description;
		this.mobEnabled = mobEnabled;
		this.mobThreshold = mobThreshold;
	}

	public void setMaxVersions(Integer maxVersions) {
		this.maxVersions = maxVersions;
	}
	
	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Integer getMaxVersions() {
		return maxVersions==null?1:maxVersions;
	}

	public void setCompressionType(Integer compressionType) {
		this.compressionType = compressionType;
	}
	
	@XmlElement
	public Boolean getMobEnabled() {
		return this.mobEnabled;
	}
		
	@XmlElement
	public Integer getMobThreshold() {
		if (this.mobEnabled != null) {
			if (this.mobEnabled == false) {
				return 0;
			}
		}
		
		return this.mobThreshold==null?0:this.mobThreshold;
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
	
	@JsonIgnore
	@XmlTransient	
	public boolean isValid() {
		boolean isvalidKey = false;
		boolean isvalidMob = false;
		boolean isvalidCompressType = false;
		
		if (keyType == PrimaryKeyUtil.ROWKEY_TYPE_HASH  || keyType == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {		
			if (keyType == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
				if (hashKeyType != null) {
					if (hashKeyType == PrimaryKeyUtil.TYPE_STRING 
							|| hashKeyType == PrimaryKeyUtil.TYPE_NUMBER 
							|| hashKeyType == PrimaryKeyUtil.TYPE_BINARY) {
						isvalidKey = true;
					}
				}
			} else if (keyType == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
				if (hashKeyType != null && rangeKeyType != null) {
					boolean isvalidhashkey = false;
					boolean isvalidrangekey = false;

					if (hashKeyType == PrimaryKeyUtil.TYPE_STRING 
							|| hashKeyType == PrimaryKeyUtil.TYPE_NUMBER 
							|| hashKeyType == PrimaryKeyUtil.TYPE_BINARY) {
						isvalidhashkey = true;
					}
					
					if (rangeKeyType == PrimaryKeyUtil.TYPE_STRING 
							|| rangeKeyType == PrimaryKeyUtil.TYPE_NUMBER 
							|| rangeKeyType == PrimaryKeyUtil.TYPE_BINARY) {
						isvalidrangekey = true;
					}
					
					isvalidKey = isvalidhashkey && isvalidrangekey;
				}
			}
		}
		
		//unit was KB
		if (mobEnabled == null) {
			mobEnabled = false;
		}
		if (mobEnabled) {
			if (mobThreshold >= RestConstants.MOB_THRESHOLD_MIN_LIMIT && mobThreshold <= RestConstants.MOB_THRESHOLD_MAX_LIMIT) {
				isvalidMob = true;
			}
		} else {
			mobThreshold = 0;
			isvalidMob = true;
		}

		isvalidCompressType = TableService.convertCompression(compressionType) == null ? false : true;
		
		return isvalidKey && isvalidMob && isvalidCompressType;
	}

	public void setMobEnabled(Boolean mobEnabled) {
		this.mobEnabled = mobEnabled;
	}

	public void setMobThreshold(Integer mobThreshold) {
		this.mobThreshold = mobThreshold;
	}
	
	@XmlElement
	public Integer getCompressionType() {		
		return compressionType==null?OtsConstants.TABLE_ALG_NONE:compressionType;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasCompressionType() {
		return TableService.convertCompression(compressionType)==null ? false : true;
	}
	
	@XmlElement
	public Integer getReplication() {
		return replication;
	}
	
	@JsonIgnore
	@XmlTransient
	public Boolean hasReplication() {
		if (replication == null) {
			return true;
		}
		return replication==0?false:true;
	}
		
	public void setReplication(Integer replication) {
		this.replication = replication;
	}
	
	@XmlElement
	public String getDescription() {
		return description==null?"":description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static TableCreateModel toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TableCreateModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TableCreateModel failed.");
		}		
    }
}
