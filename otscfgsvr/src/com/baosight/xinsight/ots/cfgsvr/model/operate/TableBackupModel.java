package com.baosight.xinsight.ots.cfgsvr.model.operate;

import java.io.ByteArrayInputStream;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
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
public class TableBackupModel implements Serializable  {
	
	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="filename")
	private String fileName;
	
	@JsonProperty(value="dst")
	private String dst;
	
	@JsonProperty(value="mode")
	private Integer mode;
	
	@JsonProperty(value="host")
	private String host;
	
	@JsonProperty(value="username")
	private String username;
	
	@JsonProperty(value="password")
	private String password;
	
	@JsonProperty(value="port")
	private Integer port;
	
	
	
	/**
	* Default constructor
	*/
	public TableBackupModel() {}
	
	public TableBackupModel(String fileName, String dst, Integer mode, String host, String username, String password, Integer port) {
		super();
		this.fileName = fileName;
		this.dst = dst;
		this.mode = mode;
		this.host = host;
		this.username = username;
		this.password = password;
		this.port = port;
	}
	
	@XmlAttribute
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	@XmlAttribute
	public String getDst() {
		return dst;
	}

	public void setDst(String dst) {
		this.dst = dst;
	}

	@XmlAttribute
	public Integer getMode() {
		return mode;
	}

	public void setMode(Integer mode) {
		this.mode = mode;
	}
	
	@XmlAttribute
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@XmlAttribute
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@XmlAttribute
	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@XmlAttribute
	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static TableBackupModel toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TableBackupModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TableBackupModel failed.");
		}		
    }
}
