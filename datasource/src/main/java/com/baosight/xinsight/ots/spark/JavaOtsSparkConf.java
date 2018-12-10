package com.baosight.xinsight.ots.spark;

public class JavaOtsSparkConf {
    // This is the tsdb-spark configuration. User can either set them in SparkConf, which
    // will take effect globally, or configure it when used, which will overwrite the value
    // set in SparkConf. If not set, the error exception got.
    final static public String OTS_TABLE_NAME = "ots.table.name";
    final static public String OTS_START_KEY = "ots.start.key";
    final static public String OTS_STOP_KEY = "ots.stop.key";
    final static public String OTS_COLUMNS = "ots.columns";

    final static public String USER_NAME = "ots.user.name";
    final static public String USER_PASSWORD = "ots.user.password";
}
