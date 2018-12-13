package com.baosight.xinsight.ots.rest.model.operate;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
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
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TenantInitModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="tenant")
	private String tenantName;
	@JsonProperty(value="tenantid")
	private Long tenantId;	
	@JsonProperty(value="adminid")
	private Long adminId;	
	@JsonProperty(value="config")
	private Map<String, Object> config;	
	
	TenantInitModel() {}
	
	public TenantInitModel(String tenantName, Long tenantId, Long adminId, Map<String, Object> config) {
		super();
		this.tenantName = tenantName;
		this.tenantId = tenantId;
		this.adminId = adminId;
		this.config = config;
	}
	
	@XmlElement
	public String getTenantName() {
		return tenantName;
	}

	public void setTenantName(String tenantName) {
		this.tenantName = tenantName;
	}
	
	@XmlElement
	public Long getTenantId() {
		return this.tenantId;
	}

	public void setTenantId(Long tenantId) {
		this.tenantId = tenantId;
	}
	
	@XmlElement
	public Long getAdminId() {
		return this.adminId;
	}

	public void setAdminId(Long adminId) {
		this.adminId = adminId;
	}
	
	public void setConfig(Map<String, Object> config){
		this.config = config;
	}
	
	@XmlElement
	public Map<String, Object> getConfig() {
		return this.config;
	}

	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static TenantInitModel toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TenantInitModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TenantInitModel failed.");
		}		
    }
}
