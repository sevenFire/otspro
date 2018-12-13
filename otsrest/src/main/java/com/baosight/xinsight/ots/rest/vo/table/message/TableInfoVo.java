package com.baosight.xinsight.ots.rest.vo.table.message;

import com.baosight.xinsight.utils.JsonUtil;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @author liyuhui
 * @date 2018/12/12
 * @description
 */
@XmlRootElement(name="tableInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableInfoVo implements Serializable {
    @JsonIgnore
    private static final long serialVersionUID = 1L;

    //如果为空，则不显示
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private long tableId;

    @JsonProperty(value="table_name")
    private String tableName;

    @JsonProperty(value="table_desc")
    private String tableDesc;

    //table_columns是一个数组，且每个元素又有多个属性
    @JsonProperty(value="table_columns")
    private List<TableColumnsInfoVo> tableColumns = new ArrayList<>();

    @JsonProperty(value="primary_key")
    private List<String> primaryKey;

    @JsonProperty(value="create_time")
    private String createTime;

    @JsonProperty(value="modify_time")
    private String modifyTime;

    @JsonProperty(value="creator")
    private String creator;

    @JsonProperty(value="modifier")
    private String modifier;

    @JsonIgnore
    private Long diskSize;

    @JsonIgnore
    private Long indexSize;

    @JsonIgnore
    private Long readCount;

    @JsonIgnore
    private Long writeCount;


    public TableInfoVo() {
    }

    public TableInfoVo(String tableName) {
        this(tableName,null,null,null,null,null,null,null);
    }

    public TableInfoVo(String tableName,
                       String tableDesc,
                       List<TableColumnsInfoVo> tableColumns,
                       List<String> primaryKey,
                       String createTime,
                       String modifyTime,
                       String creator,
                       String modifier) {
        this.tableName = tableName;
        this.tableDesc = tableDesc;
        this.tableColumns = tableColumns;
        this.primaryKey = primaryKey;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
        this.creator = creator;
        this.modifier = modifier;
    }

    public TableInfoVo(String tableName,
                       String tableDesc,
                       List<TableColumnsInfoVo> tableColumns,
                       List<String> primaryKey,
                       String createTime,
                       String modifyTime,
                       String creator,
                       String modifier,
                       Long diskSize,
                       Long indexSize,
                       Long readCount,
                       Long writeCount) {
        this(tableName,tableDesc,tableColumns,primaryKey,createTime,modifyTime,creator,modifier);
        this.diskSize = diskSize;
        this.indexSize = indexSize;
        this.readCount = readCount;
        this.writeCount = writeCount;
    }

    @JsonIgnore
    @XmlTransient
    @Override
    public String toString() {
        return JsonUtil.toJsonString(this);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(String tableDesc) {
        this.tableDesc = tableDesc;
    }

    public List<TableColumnsInfoVo> getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(List<TableColumnsInfoVo> tableColumns) {
        this.tableColumns = tableColumns;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getModifier() {
        return modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier;
    }

    public Long getDiskSize() {
        return diskSize;
    }

    public void setDiskSize(Long diskSize) {
        this.diskSize = diskSize;
    }

    public Long getIndexSize() {
        return indexSize;
    }

    public void setIndexSize(Long indexSize) {
        this.indexSize = indexSize;
    }

    public Long getReadCount() {
        return readCount;
    }

    public void setReadCount(Long readCount) {
        this.readCount = readCount;
    }

    public Long getWriteCount() {
        return writeCount;
    }

    public void setWriteCount(Long writeCount) {
        this.writeCount = writeCount;
    }
}
