package com.baosight.xinsight.ots.rest.vo.table.message;

import org.codehaus.jackson.annotate.JsonProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author liyuhui
 * @date 2018/12/13
 * @description
 */
@XmlRootElement(name="tableColumnsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableColumnsInfoVo {

    @JsonProperty(value="col_name")
    private String colName;

    @JsonProperty(value="col_type")
    private String colType;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }
}
