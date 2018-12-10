package com.baosight.xinsight.ots.spark;

import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import net.iharder.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class JavaOtsDataSourceTest {

    public static final Logger LOG = Logger.getLogger(JavaOtsDataSourceTest.class);

    public static void main(String[] args) {
        System.out.println("========Test JavaOtsDataSourceTest==========");
        String tableName = "test";

        String startKey = "0000"; //hex
        String stopKey = "ffff"; //hex
        try {
            startKey = Bytes.toHex(PrimaryKeyUtil.buildPrimaryKey("0000", PrimaryKeyUtil.TYPE_STRING,
                    "", PrimaryKeyUtil.TYPE_STRING));
            stopKey = Bytes.toHex(PrimaryKeyUtil.buildPrimaryKey("ffff", PrimaryKeyUtil.TYPE_STRING,
                    "", PrimaryKeyUtil.TYPE_STRING)); //hex
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Map<String, String> options = new HashMap<>();
        options.put(JavaOtsSparkConf.USER_NAME, "admin@root");
        options.put(JavaOtsSparkConf.USER_PASSWORD, "admin");

        options.put(JavaOtsSparkConf.OTS_TABLE_NAME, tableName);
        options.put(JavaOtsSparkConf.OTS_COLUMNS, "column1:Int,column2:String");
        options.put(JavaOtsSparkConf.OTS_START_KEY, startKey);
        options.put(JavaOtsSparkConf.OTS_STOP_KEY, stopKey);

        SparkConf sparkConf = new SparkConf().setAppName("JavaOtsDataSourceTest").setMaster("local");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrameReader reader = sqlContext.read();
        DataFrameReader readerOp = reader.options(options);
        DataFrameReader readerFormat = readerOp.format("com.baosight.xinsight.ots.spark");
        DataFrame df0 = readerFormat.load();
        df0.limit(2).show();

        DataFrame df1 = sqlContext.read().options(options).format("com.baosight.xinsight.ots.spark").load();
        df1.registerTempTable("sql_temp");
        sqlContext.sql("select * from sql_temp limit 2").show();

        df1.limit(1).show();

        sc.stop();
    }
}
