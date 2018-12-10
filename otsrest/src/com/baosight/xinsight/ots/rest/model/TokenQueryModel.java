package com.baosight.xinsight.ots.rest.model;

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
public class TokenQueryModel  implements Serializable  {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="token")
	private String token;
	
	@JsonProperty(value="errcode")
	private Long errcode;

	/**
	* Default constructor
	*/
	public TokenQueryModel() {}

	public TokenQueryModel(String token, Long errcode) {
		super();
		this.token = token;
		this.errcode = errcode;
	}

	@XmlAttribute	
	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
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
    public static TokenQueryModel toClass(String in) throws OtsException {		
		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, TokenQueryModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to QueryTokenModel failed.");
		}
    }
}
