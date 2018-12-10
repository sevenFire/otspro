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

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.model.IndexInfoModel;
import com.baosight.xinsight.utils.JsonUtil;


@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class IndexUpdateModel implements Serializable{

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	//only choose one follows, important
	@JsonProperty(value="truncate")
	private Boolean truncate;	//optional
	
	@JsonProperty(value="rebuild")
	private Boolean rebuild;	//optional
	
	@JsonProperty(value="rebuildinfo")
	@XmlElement(name="rebuildinfo")
	private IndexInfoModel rebuildmodel; //optional

	public IndexUpdateModel() {}
	
	public IndexUpdateModel(Boolean truncate, Boolean rebuild) {
		super();
		this.truncate = truncate;
		this.rebuild = rebuild;
	}
	
	public IndexUpdateModel(Boolean truncate, Boolean rebuild, IndexInfoModel rebuildmodel) {
		super();
		this.truncate = truncate;
		this.rebuild = rebuild;
		this.rebuildmodel = rebuildmodel;
	}
	
	@XmlElement
	public Boolean getTruncate() {
		return truncate;
	}

	public void setTruncate(Boolean truncate) {
		this.truncate = truncate;
	}

	@JsonIgnore
	@XmlTransient
	public boolean hasTruncate() {
		return truncate==null?false:true;
	}
	
	@XmlElement
	public Boolean getRebuild() {
		return rebuild;
	}

	public void setRebuild(Boolean rebuild) {
		this.rebuild = rebuild;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasRebuild() {
		return rebuild==null?false:true;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasRebuildInfo() {
		return rebuildmodel==null?false:true;
	}
	
	public IndexInfoModel getRebuildmodel() {
		return rebuildmodel;
	}

	public void setRebuildmodel(IndexInfoModel rebuildmodel) {
		this.rebuildmodel = rebuildmodel;
	}
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static IndexUpdateModel toClass(String in) throws OtsException {		
		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, IndexUpdateModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to IndexUpdateModel failed.");
		}
    }
}
