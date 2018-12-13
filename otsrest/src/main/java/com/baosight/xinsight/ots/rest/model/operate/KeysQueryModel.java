package com.baosight.xinsight.ots.rest.model.operate;

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


/**
 * 该类仅仅是url参数组合类
 * 
 * @author huangming
 *
 */

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class KeysQueryModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="key_list")
	private List<KeyModel> keylist = new ArrayList<KeyModel>();

	public KeysQueryModel() {}
		
	public KeysQueryModel(List<KeyModel> keylist) {		
		super();
		this.keylist = keylist;
	}
		
	@XmlElement
	public List<KeyModel> getKeylist() {
		return keylist;
	}

	public void setKeylist(List<KeyModel> keylist) {
		this.keylist = keylist;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }

	@JsonIgnore
	@XmlTransient
	public static KeysQueryModel toClass(String in) throws OtsException {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, KeysQueryModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to KeysQueryModel failed.");
		}	
	}
}
