package com.baosight.xinsight.ots.rest.body.table;

import com.baosight.xinsight.utils.JsonUtil;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @author liyuhui
 * @date 2018/12/12
 * @description 表的列信息
 */
@XmlRootElement(name="tableColumns")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableColumnsBodyModel implements Serializable{

    @JsonIgnore
    private static final long serialVersionUID = 1L;

    @JsonProperty(value="col_name")
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String colName;

    @JsonProperty(value="col_type")
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String colType;

    public TableColumnsBodyModel() {
    }

    public TableColumnsBodyModel(String colName, String colType) {
        this.colName = colName;
        this.colType = colType;
    }

    @JsonIgnore
    @XmlTransient
    @Override
    public String toString() {
        return JsonUtil.toJsonString(this);
    }

    @XmlElement
    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    @XmlElement
    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }

}
