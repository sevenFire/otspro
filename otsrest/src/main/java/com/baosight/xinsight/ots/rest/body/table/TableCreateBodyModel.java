package com.baosight.xinsight.ots.rest.body.table;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.metacfg.Table;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.utils.JsonUtil;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @author liyuhui
 * @date 2018/12/12
 * @description 创建表的body
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TableCreateBodyModel implements Serializable{
    @JsonIgnore
    private static final long serialVersionUID = 1L;

    @JsonProperty(value="primary_key")
    private List<String> primaryKey;

    //table_columns是一个数组，且每个元素又有多个属性
    @JsonProperty(value="table_columns")
    private List<TableColumnsBodyModel> tableColumns = new ArrayList<TableColumnsBodyModel>();

    @JsonProperty(value="table_desc")
    private String tableDesc;

    public TableCreateBodyModel() {
    }

    public TableCreateBodyModel(List<TableColumnsBodyModel> tableColumns, String tableDesc) {
        this.tableColumns = tableColumns;
        this.tableDesc = tableDesc;
    }

    @JsonIgnore
    @XmlTransient
    @Override
    public String toString() {
        return JsonUtil.toJsonString(this);
    }

    /**
     * 将json串转换成实体类
     * @param in
     * @return
     * @throws OtsException
     */
    @JsonIgnore
    @XmlTransient
    public static TableCreateBodyModel toClass(String in) throws OtsException {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(in.getBytes(OtsConstants.DEFAULT_ENCODING));
            return JsonUtil.readJsonFromStream(byteArrayInputStream, TableCreateBodyModel.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_JSON2OBJECT, "convert json input to TableCreateModel failed.");
        }
    }



    @XmlElement
    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

//    @XmlElement
//    public List<String> getTableColumns() {
//        List<String> tableColumnsString = new ArrayList<>();
//        Iterator<TableColumnsBodyModel> iterable = tableColumns.iterator();
//        while (iterable.hasNext()){
//            tableColumnsString.add(iterable.next().toString());
//        }
//        return tableColumnsString;
//    }

    @XmlElement
    public List<TableColumnsBodyModel> getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(List<TableColumnsBodyModel> tableColumns) {
        this.tableColumns = tableColumns;
    }

    public String getTableDesc() {
        return tableDesc;
    }

    @XmlElement
    public void setTableDesc(String tableDesc) {
        this.tableDesc = tableDesc;
    }

    /**
     * 将请求体中的参数放入table的实体类中
     * @return
     */
    public Table toTable() {
        Table table = new Table();
        table.setTableDesc(tableDesc);
        table.setPrimaryKey(primaryKey.toString());
        table.setTableColumns(tableColumns.toString());

        return table;
    }

    @Test
    public void test() {
        Table table = new Table();
        table.setTableDesc(tableDesc);
        table.setPrimaryKey(primaryKey.toString());
        table.setTableColumns(tableColumns.toString());

        System.out.println("ok");
    }
}
