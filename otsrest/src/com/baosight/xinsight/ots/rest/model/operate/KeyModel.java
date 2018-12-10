package com.baosight.xinsight.ots.rest.model.operate;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;


/**
 * 该类仅仅是url参数组合类
 * 
 * @author huangming
 *
 */

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class KeyModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty("hash_key")	
	private String hashkey;
	
	@JsonProperty("range_key")	
	private String rangekey;

	public KeyModel() {}
	
	public KeyModel(String hashkey, String rangekey) {		
		super();
		this.hashkey = hashkey;
		this.rangekey = rangekey;		
	}
			
	@XmlElement
	public String getHashkey() {
		return hashkey;
	}

	public void setHashkey(String hashkey) {
		this.hashkey = hashkey;
	}
	
	@XmlElement
	public String getRangekey() {
		return rangekey;
	}

	public void setRangekey(String rangekey) {
		this.rangekey = rangekey;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
}
