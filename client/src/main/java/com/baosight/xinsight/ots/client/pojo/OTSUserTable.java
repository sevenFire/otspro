package com.baosight.xinsight.ots.client.pojo;

import java.util.Date;

/**
 * @author liyuhui
 * @date 2018/12/11
 * @description table对应的实体类
 */
public class OTSUserTable {
    private Long tableId ; //表id
    private Long userId ; //用户id
    private Long tenantId; //租户id
    private String tableName; //表名
    private String tableDesc; //表的描述
    private String primaryKey; //表的主键
    private String tableColumns; //表的列信息
    private Date createTime; //创建时间
    private Date modifyTime; //最近修改时间
    private Long creator; //创建人
    private Long modifier; //最近修改人

    //todo lyh
//    private Boolean permission; //权限信息
//    private Boolean enable; //是否启用

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
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

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(String tableColumns) {
        this.tableColumns = tableColumns;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public Long getCreator() {
        return creator;
    }

    public void setCreator(Long creator) {
        this.creator = creator;
    }

    public Long getModifier() {
        return modifier;
    }

    public void setModifier(Long modifier) {
        this.modifier = modifier;
    }

//    public Boolean getEnable() {
//        return enable;
//    }
//
//    public void setEnable(Boolean enable) {
//        this.enable = enable;
//    }
}
