package com.baosight.xinsight.ots.rest.database;

import com.baosight.xinsight.ots.client.pojo.OTSUserTable;
import com.baosight.xinsight.ots.constants.TableConstants;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author liyuhui
 * @date 2018/12/12
 * @description 通过反射自动拼接成sql语句，即使后续表结构变动，也无需手动修改sql语句
 */

public class SqlUtil {
//    private static final Logger LOG = Logger.getLogger(SqlUtil.class);


    public static final String KEY_COLUMNS = "columns";
    public static final String KEY_VALUES = "values";
    public static final String KEY_COLUMN_EQUALS = "columnEquals";

    /**
     * 根据tableName和操作方法生成sql语句
     * @param tableName
     * @return
     */
    public static StringBuilder getSql(String tableName,String entityName, String operator) throws NullPointerException{
        StringBuilder sql = new StringBuilder();

        //ots_user_table表
        if(TableConstants.T_OTS_USER_TABLE.equals(tableName)){
            if(TableConstants.INSERT.equals(operator)){
                Map columnMap = getColumn(entityName);
                sql.append(" INSERT INTO " + tableName + columnMap.get(KEY_COLUMNS)
                        + " VALUES " + columnMap.get(KEY_VALUES));
            }else{
                //todo lyh other situation
            }
        }
//        LOG.debug(sql);

        if( "".equals(sql)){
            throw new NullPointerException("sql为空。tableName："+tableName + "operator:" + operator);
        }

        return sql;
    }


    /**
     * 根据实体属性名，拼接sql语句
     * @param entityName 实体名
     * @return 用于sql的语句
     */
    private static Map getColumn(String entityName){
        Map columnMap = new HashMap();

        //列名，用于query和insert语句
        StringBuilder columnSb = new StringBuilder(" ( ");
        //（?,?,?），用于update语句
        StringBuilder valuesSb = new StringBuilder(" ( ");
        //列名= ?，用于update语句
        StringBuilder columnEqualsSb = new StringBuilder(" ");

        List<String> filedNames = null;
        if(TableConstants.E_OTS_USER_TABLE.equals(entityName)){
            filedNames = getFiledName(new OTSUserTable());
        }else{
            //todo lyh other situation
        }

        if(filedNames !=null){
            for(int i=0; i < filedNames.size();i++){
                columnSb.append(filedNames.get(i));
                valuesSb.append(" ? ");
                columnEqualsSb.append(filedNames.get(i)+" =? ");

                if(i < filedNames.size()-1){
                    columnSb.append(" , ");
                    valuesSb.append(" , ");
                    columnEqualsSb.append(" , ");
                }else{
                    columnSb.append(" ) ");
                    valuesSb.append(" ) ");
                    columnEqualsSb.append(" ");
                }
            }
        }

        columnMap.put(KEY_COLUMNS, columnSb.toString());
        columnMap.put(KEY_VALUES, valuesSb.toString());
        columnMap.put(KEY_COLUMN_EQUALS, columnEqualsSb.toString());
        return columnMap;
    }

    /**
     * 拿到实体类的属性名数组
     * @param o 实体类
     * @return 实体类的属性名数组
     */
    private static List<String> getFiledName(Object o){
        Field[] fields = o.getClass().getDeclaredFields();
        List<String> fieldNames=new ArrayList<>();
        for(int i=0;i<fields.length;i++){
            fieldNames.add(fields[i].getName());
        }
        return fieldNames;
    }

    public static void main(String[] args) {
        String sql = getSql(TableConstants.T_OTS_USER_TABLE,TableConstants.E_OTS_USER_TABLE, TableConstants.INSERT).toString();
        System.out.println(sql);
    }
}
