package com.baosight.xinsight.ots.rest.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.utils.JsonUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class IndexInfoModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;
	
	@JsonProperty(value="errcode")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long errcode;
	
	@JsonProperty(value="index_name")
	private String name;
	
	@JsonProperty(value="type")
	private int type;//optional

	@JsonProperty(value="shard_num")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Integer shard; //for solr 

	@JsonProperty(value="replication_num")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Integer replication; //default 1
	
	@JsonProperty(value="columns")
	@XmlElement(name="columns")
	private List<ColumnModel> columnList = new ArrayList<ColumnModel>();

	@JsonProperty(value="pattern")
	@XmlElement(name="pattern")
	private Integer indexPattern; //1 default
	
	@JsonProperty(value="start_key")
	@XmlElement(name="start_key")
	private String startKey;//optional
	
	@JsonProperty(value="end_key")
	@XmlElement(name="end_key")
	private String endKey; //optional
	
	@JsonProperty(value="create_time")
	@XmlElement(name="create_time")
	private String createTime;//optional
	
	@JsonProperty(value="last_modify")
	@XmlElement(name="last_modify")
	private String lastModify; //optional
	
	@JsonProperty(value="table_id")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long tableId;
	
	@JsonProperty(value="index_id")
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL) //if null, will not show in the results
	private Long indexId;
	
	/**
	 * Default constructor
	 */
	public IndexInfoModel() {}
	
	public IndexInfoModel(String name, Integer indexPattern, List<ColumnModel> columnList) {
		this(name, RestConstants.OTS_INDEX_TYPE_SOLR, indexPattern, null, null, columnList);
	}
	
	public IndexInfoModel(String name, int type, Integer indexPattern,
			String startKey, String endKey, List<ColumnModel> columnList) {
		super();
		this.name = name;
		this.columnList = columnList;
		this.indexPattern = indexPattern;
		this.startKey = startKey;
		this.endKey = endKey;
		this.type = type;
	}	
	
	@XmlElement
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@XmlElement
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	@XmlElement
	public List<ColumnModel> getColumnList() {				
		return this.columnList;
	}

	public void setColumnList(List<ColumnModel> columnList) {
		this.columnList = columnList;
	}	
	
	@JsonIgnore
	@XmlTransient
	public List<ColumnModel> getColumnListForSolr() {
		List<ColumnModel> indexColumnList = new ArrayList<ColumnModel>(); 
		indexColumnList = this.columnList;
		
		//interchange
		for (ColumnModel model : indexColumnList) {			
			if (model.getType().equals("int32")) {
				model.setType("int");
			} else if (model.getType().equals("int64")) {
				model.setType("long");
			} else if (model.getType().equals("float32")) {
				model.setType("float");
			} else if (model.getType().equals("float64")) {
				model.setType("double");
			}
		}
		
		return indexColumnList;
	}

	@JsonIgnore
	@XmlTransient
	public void setColumnListForSolr(List<ColumnModel> columnList) {
		this.columnList = columnList;
		
		//interchange
		for (ColumnModel model : this.columnList) {
			if (model.getType().equals("int")) {
				model.setType("int32");
			} else if (model.getType().equals("long")) {
				model.setType("int64");
			} else if (model.getType().equals("float")) {
				model.setType("float32");
			} else if (model.getType().equals("double")) {
				model.setType("float64");
			}
		}
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
	public void addColumn(ColumnModel c) {
		if (c != null) {
			columnList.add(c);
		}
	}
	
	@XmlElement
	public Long getTableId() {
		return tableId;
	}

	public void setTableId(Long tableId) {
		this.tableId = tableId;
	}

	@XmlElement
	public Long getIndexId() {
		return indexId;
	}

	public void setIndexId(Long indexId) {
		this.indexId = indexId;
	}

	@JsonIgnore
	@XmlTransient
	public void clear() {
		columnList.clear();
	}
	
	@JsonIgnore
	@XmlTransient
	public void size() {
		columnList.size();
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasStartkey() {
		if (startKey==null) {
			return false;
		}
		
		if (!startKey.isEmpty()) {
			return true;
		}
		
		return false;
	}
	
	@JsonIgnore
	@XmlTransient
	public boolean hasEndkey() {
		if (endKey==null) {
			return false;
		}
		
		if (!endKey.isEmpty()) {
			return true;
		}
		
		return false;
	}	
	
	public Integer getIndexPattern() {
		return indexPattern;
	}

	public void setIndexPattern(Integer indexPattern) {
		this.indexPattern = indexPattern;
	}

	public String getStartKey() {
		return startKey;
	}

	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}

	public String getEndKey() {
		return endKey;
	}

	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}
	
	public Integer getShard() {
		if (shard==null) {
			try{
				shard = Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_SHARD_NUM, "3"));
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		return shard;
	}
	
	public void setShard(Integer shard) {
		this.shard = shard;
	}
	
	public Integer getReplication() {
		if (replication==null) {
			try{
				replication = Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_REPLICATION_FACTOR, "1"));
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		return replication;
	}

	public void setReplication(Integer replication) {
		this.replication = replication;
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
	public boolean checkColumnsDuplicate() {
		Map<String, String> realColumnsMap = new HashMap<String, String>();
		for (ColumnModel colmodel : columnList) {
			realColumnsMap.put(colmodel.getName(), colmodel.getType());
		}
		
		return !(realColumnsMap.size() == columnList.size());
	}	
	
	@JsonIgnore
	@XmlTransient
	@Override
    public String toString() {
		return JsonUtil.toJsonString(this);
    }
	
	@JsonIgnore
	@XmlTransient	
    public static IndexInfoModel toClass(String in) throws OtsException {		
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
			return JsonUtil.readJsonFromStream(bais, IndexInfoModel.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to IndexInfoModel failed.");
		}
    }
}
