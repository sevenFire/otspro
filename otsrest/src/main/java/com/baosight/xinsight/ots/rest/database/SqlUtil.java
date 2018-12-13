package com.baosight.xinsight.ots.rest.database;
import com.baosight.xinsight.ots.constants.TableConstants;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liyuhui
 * @date 2018/12/12
 * @description 通过反射自动拼接成sql语句，即使后续表结构变动，也无需手动修改sql语句
 */

public class SqlUtil {
//    private static final Logger LOG = Logger.getLogger(SqlUtil.class);


    private static final String KEY_COLUMNS = "columns";
    private static final String KEY_VALUES = "values";
    private static final String KEY_COLUMN_EQUALS = "columnEquals";
    private static final String[] OTS_USER_TABLE_COLUMNS = new String[]{"table_id","user_id","tenant_id","table_name","table_desc","primary_key","table_columns","create_time","modify_time","creator","modifier"};

    /**
     * 根据tableName和操作方法生成sql语句
     * @param tableName
     * @return
     */
    public static StringBuilder getSql(String tableName,String entityName, String operator) throws NullPointerException{
        StringBuilder sql = new StringBuilder();

        //insert 操作
        if(TableConstants.INSERT.equals(operator)) {
            //拼接sql
            List<String> filedName = getFiledName(entityName, operator);
            Map columnMap = getColumn(filedName);
            sql.append(" INSERT INTO " + tableName + columnMap.get(KEY_COLUMNS)
                    + " VALUES " + columnMap.get(KEY_VALUES));
        }else{
            //todo lyh other operation
        }

        if( "".equals(sql)){
            throw new NullPointerException("sql为空。tableName："+tableName + "operator:" + operator);
        }

        return sql;
    }


    /**
     * 根据字段名，拼接sql语句
     * @param filedNames
     * @return 用于sql的语句
     */
    private static Map getColumn(List<String> filedNames){
        Map columnMap = new HashMap();

        //列名，用于query和insert语句
        StringBuilder columnSb = new StringBuilder(" ( ");
        //（?,?,?），用于update语句
        StringBuilder valuesSb = new StringBuilder(" ( ");
        //列名= ?，用于update语句
        StringBuilder columnEqualsSb = new StringBuilder(" ");

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

//    /**
//     * 拿到实体类的属性名数组
//     * @param o 实体类
//     * @return 实体类的属性名数组
//     */
//    private static List<String> getFiledName(Object o){
//        Field[] fields = o.getClass().getDeclaredFields();
//        List<String> fieldNames=new ArrayList<>();
//        for(int i=0;i<fields.length;i++){
//            fieldNames.add(fields[i].getName());
//        }
//        return fieldNames;
//    }

        /**
     * 拿到表的字段名数组
     * @param entityName 实体名
     * @return 实体类的属性名数组
     */
    private static List<String> getFiledName(String entityName, String operator){
        List<String> fieldNames = null;

        if(TableConstants.E_OTS_USER_TABLE.equals(entityName)){
            fieldNames = Arrays.asList(OTS_USER_TABLE_COLUMNS);
        }else{
            //todo lyh other situation
        }

        if (TableConstants.INSERT.equals(operator)){
            fieldNames.remove(0);//插入时需要去掉id。
        }
        return fieldNames;
    }


    public static void main(String[] args) {
        String sql = getSql(TableConstants.T_OTS_USER_TABLE,TableConstants.E_OTS_USER_TABLE, TableConstants.INSERT).toString();
        System.out.println(sql);
    }
}
