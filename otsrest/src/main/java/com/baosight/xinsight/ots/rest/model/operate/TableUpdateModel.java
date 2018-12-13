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
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TableUpdateModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonIgnore
	//@JsonProperty(value="enabled")
	private Boolean enable;		//optional
	
	@JsonIgnore
	//@JsonProperty(value="max_versions")
	private Integer maxVersions;//optional
	
	@JsonIgnore
	//@JsonProperty(value="replication")	
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Boolean replication;	//optional
	
	@JsonProperty(value="description")
	private String description;	//optional
	
	@JsonProperty(value="mob_enabled")
	private Boolean mobEnabled;	//optional
	
	@JsonProperty(value="mob_threshold")
	private Integer mobThreshold;	//optional
	
	public TableUpdateModel() {}
	
	public TableUpdateModel(Integer maxVersions) {
		this(null, maxVersions, null);
	}
	
	public TableUpdateModel(String description) {
		this(null, null, description);
	}

	public TableUpdateModel(Boolean enable,
			Integer maxVersions, String description) {
		this(enable, maxVersions, null, null, description);
	}	
	
	public TableUpdateModel(Boolean enable,
			Integer maxVersions, Boolean mobEnabled, Integer mobThreshold, String description) {
		super();
		this.enable = enable;
		this.maxVersions = maxVersions;
		this.description = description;
		this.mobEnabled = mobEnabled;
		this.mobThreshold = mobThreshold;
	}	

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Integer getMaxVersions() {
		return maxVersions==null?1:maxVersions;
	}

	public void setMaxVersions(Integer maxVersions) {
		this.maxVersions = maxVersions;
	}

	@JsonIgnore
	@XmlTransient
	//@XmlElement
	public Boolean getReplication() {
		return replication;
	}

	public void setReplication(Boolean replication) {
		this.replication = replication;
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

	@XmlElement
	public Boolean getMobEnabled() {
		return this.mobEnabled;
	}
	
	public void setMobEnabled(Boolean mobEnabled){
		this.mobEnabled = mobEnabled;
	}
	
	@XmlElement
	public Integer getMobThreshold() {
		return this.mobThreshold;
	}
	
	public void setModThreshold(Integer mobThreshold){
		this.mobThreshold = mobThreshold;
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
	public boolean isValid(boolean oldMobEnabled) {
		boolean isvalidMob = false;	
		
		//unit was KB
		if (oldMobEnabled) {
			if (mobEnabled != null) {
				if (mobEnabled) {
					if (mobThreshold >= RestConstants.MOB_THRESHOLD_MIN_LIMIT && mobThreshold <= RestConstants.MOB_THRESHOLD_MAX_LIMIT) {
						isvalidMob = true;
					}
				}
			} else {
				isvalidMob = true;
			}
		} else {
			mobEnabled = null;
			mobThreshold = null;
			isvalidMob = true;
		}
		
		return isvalidMob;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static TableUpdateModel toClass(String in) throws OtsException {	
		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TableUpdateModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TableUpdateModel failed.");
		}
    }
}
