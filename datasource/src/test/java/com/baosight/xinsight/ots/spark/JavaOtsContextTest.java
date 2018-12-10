package com.baosight.xinsight.ots.spark;

import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class JavaOtsContextTest {

    public static final Logger LOG = Logger.getLogger(JavaOtsContextTest.class);

    public static void main(String[] args) {
        System.out.println("========Test JavaOtsContextTest==========");
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

        Configuration conf = HBaseConfiguration.create();
        conf.set(JavaOtsSparkConf.USER_NAME, "admin@root");
        conf.set(JavaOtsSparkConf.USER_PASSWORD, "admin");

        conf.set(JavaOtsSparkConf.OTS_TABLE_NAME, tableName);
        conf.set(JavaOtsSparkConf.OTS_COLUMNS, "column1:Int,column2:String");
        conf.set(JavaOtsSparkConf.OTS_START_KEY, startKey);
        conf.set(JavaOtsSparkConf.OTS_STOP_KEY, stopKey);

        SparkConf sparkConf = new SparkConf().setAppName("JavaOtsContextTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaOtsContext otsContext = new JavaOtsContext(sc, conf);
        JavaRDD<OtsRowRecord> rdd = otsContext.rdd();
        rdd.foreach(new VoidFunction<OtsRowRecord>() {
            @Override
            public void call(OtsRowRecord rec) {
                System.out.println("#########" + rec.toString());
            }
        });

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(otsContext.rowRDD(), otsContext.otsSchema());
        df.registerTempTable("tmp_table");
        sqlContext.sql("SELECT * FROM tmp_table limit 5").show();

        DataFrame df2 = otsContext.dataFrame();
        System.out.println(df2.schema().treeString());
        String[] columns = df2.columns();
        System.out.print("[");
        for (int i = 0; i < columns.length - 1; i++)
            System.out.print(columns[i] + ", ");
        System.out.print(columns[columns.length - 1]);
        System.out.println("]");
        //df2.registerTempTable("tmp_table2");
        //df2.sqlContext.sql("select * from tmp_table2 limit 5").show();

        System.out.println("++++++++++++++++");
        System.out.println(otsContext.otsSchema().treeString());
        System.out.println(otsContext.otsSchema().toString());

        sc.stop();
    }
}
