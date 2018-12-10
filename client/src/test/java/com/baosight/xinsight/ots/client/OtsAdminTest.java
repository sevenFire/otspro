package com.baosight.xinsight.ots.client;


import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.config.ConfigReader;
import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.client.table.RecordQueryOption;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.table.RowCell;
import com.baosight.xinsight.ots.client.table.RowRecord;
import com.baosight.xinsight.ots.exception.OtsException;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OtsAdminTest {
    private OtsAdmin otsAdmin;

    @Before
    public void init(){
        ConfigReader configReader = new ConfigReader("ots", ConfigReader.class);
        OtsConfiguration conf = OtsConfiguration.create();
        //如果默认ots的conf.properties存在,这样都可以不配置
        conf.setProperty(OtsConstants.ZOOKEEPER_QUORUM, configReader.getValue(ConfigConstants.ZOOKEEPER_QUORUM, "127.0.0.1:2181"));
        conf.setProperty(OtsConstants.ZOOKEEPER_TIMEOUT, configReader.getValue(ConfigConstants.ZOOKEEPER_TIMEOUT, "3000"));
//		conf.setProperty(OtsConstants.CLIENT_HBASE_RETRIES_NUMBER, ConfigUtil.getValue("hbase_client_retries_number", "1"));
        conf.setProperty(OtsConstants.CLIENT_HBASE_RETRIES_NUMBER, "2");
        conf.setProperty(OtsConstants.POSTGRES_QUORUM, "168.2.8.221");
        conf.setProperty(OtsConstants.POSTGRES_PORT, "5432");
        conf.setProperty(OtsConstants.POSTGRES_DBNAME, configReader.getValue(ConfigConstants.OTS_DBNAME, "ots"));
        conf.setProperty(OtsConstants.POSTGRES_USERNAME, configReader.getValue(ConfigConstants.POSTGRESQL_USER, "postgres"));
        conf.setProperty(OtsConstants.POSTGRES_PASSWORD, configReader.getValue(ConfigConstants.POSTGRESQL_PASSWORD, "q1w2e3"));
//        conf.setProperty(OtsConstants.INDEX_CONFIG_HOME, getValue("ots_indexer_cfghome");
        conf.setProperty(OtsConstants.HBASE_INDEXER_QUORUM, configReader.getValue(ConfigConstants.OTS_INDEX_HOST, "127.0.0.1"));
        conf.setProperty(ConfigConstants.YARN_RM_HOST, configReader.getValue(ConfigConstants.YARN_RM_HOST, "127.0.0.1:8088"));
        conf.setProperty(ConfigConstants.REDIS_QUORUM, configReader.getValue(ConfigConstants.REDIS_QUORUM, "127.0.0.1:6379"));

        try {
            otsAdmin = new OtsAdmin(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
    * 创建小表
    * createTable在创建数据库中的小表，在这个函数里就包含了先判断hbase大表是否存在
    */
    public void createTableTest(){
        try {
            long userid = 12345;
            long tenantid = 101;
            String tablename = "test_wls1";
            if (!otsAdmin.isTableExist(userid, tenantid, tablename)){
                OtsTable otstable = otsAdmin.createTable(userid, tenantid, tablename, 2, 1, 2, tablename, "snappy", 1, true, 0, true);
                boolean isTableExist_after = otsAdmin.isTableExist(otstable.getUserid(), otstable.getTenantid(), otstable.getName());
                System.out.println("Create the table："+ otstable.getName());
                Assert.assertTrue("Create a table!",isTableExist_after);
            }
            else
            {
                System.out.println("The table has existed!");
            }

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    /**
     * 删除小表
     */
    public void deleteTableTest(){
        try{
            long userid = 12345;
            long tenantid = 101;
            String tablename_notExist = "test_wls2";
            String tablename_Exist = "test_wls1";
            if(otsAdmin.isTableExist(userid, tenantid, tablename_Exist)){
                otsAdmin.deleteTable(userid, tenantid, tablename_Exist);
            }
            else if(!otsAdmin.isTableExist(userid, tenantid, tablename_notExist)){
                System.out.println("The table doesn't existed!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    /**
    * 插记录到小表中
     */
    public void insertRecordTest(){
        try {
            long userid = 12345;
            long tenantid = 101;
            String tablename_notExist = "test_wls2";
            String tablename_Exist = "test_wls";

            //  若小表不存在，无法插入记录
            OtsTable otstable_notExist = otsAdmin.getTable(userid, tenantid, tablename_notExist);
            if(otstable_notExist == null) {
                System.out.println("The table doesn't exist!");
            }
            Assert.assertNull("The table doesn't exist!",otstable_notExist);

            //  若小表存在，往里插入记录
            OtsTable otstable = otsAdmin.getTable(userid, tenantid, tablename_Exist);
            if(otstable != null){

                RowRecord rec1 = new RowRecord();
                rec1.setRowkey("111110".getBytes());
                RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
                rec1.addCell(cell1);
                otstable.insertRecord(rec1);
                System.out.println("Insert a record into the table " + otstable.getName() );
            }

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 查询记录
     */
    @Test
    public void getRecordsTest(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
//            插入记录
//            RowRecord rec1 = new RowRecord();
//            rec1.setRowkey("10000".getBytes());
//            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
//            rec1.addCell(cell1);
//            otstable.insertRecord(rec1);

            //用RecordQueryOption查询，要设Cursor_mark和Limit
            RecordQueryOption recordQuery = new RecordQueryOption();
            recordQuery.setCursor_mark("*");
            recordQuery.setLimit(100L);
            RecordResult r1 = otstable.getRecords(recordQuery);


            List<RowRecord> ff = r1.getListRecords();
            for(int i = 0;i < ff.size();i++){
                System.out.println("1_lists:" + ff.get(i).toString());
            }

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        }


    }

    /**
     * 查询记录
     */
    @Test
    public void getRecords_RangeKeys_Test(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            //插入记录
            RowRecord rec1 = new RowRecord();
            rec1.setRowkey("10000".getBytes());
            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
            rec1.addCell(cell1);
            otstable.insertRecord(rec1);

            RowRecord rec2 = new RowRecord();
            rec2.setRowkey("20000".getBytes());
            RowCell cell2 = new RowCell("col2".getBytes(), "test2".getBytes());
            rec2.addCell(cell2);
            otstable.insertRecord(rec2);

            //用行键（range）和RecordQueryOption查询
            RecordQueryOption recordQuery_rangeRK = new RecordQueryOption();
            recordQuery_rangeRK.setLimit(100L);

            byte[] startKey = otsAdmin.getBigRowkey(otstable.getId(),rec1.getRowkey());
            byte[] endKey = otsAdmin.getBigRowkey(otstable.getId(),rec2.getRowkey());
            RecordResult r_rangeRK = otstable.getRecords(startKey, endKey, recordQuery_rangeRK);

            List<RowRecord> ff2 = r_rangeRK.getListRecords();
            for(int i = 0;i < ff2.size();i++){
                System.out.println("Rangekey_lists:" + ff2.get(i).toString());
            }


        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * 查询记录
     */
    @Test
    public void getRecords_ListKeys_Test(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            RowRecord rec3 = new RowRecord();
            rec3.setRowkey("30000".getBytes());
            RowCell cell3 = new RowCell("col3".getBytes(), "test3".getBytes());
            rec3.addCell(cell3);
            otstable.insertRecord(rec3);

            RowRecord rec4 = new RowRecord();
            rec4.setRowkey("40000".getBytes());
            RowCell cell4 = new RowCell("col4".getBytes(), "test4".getBytes());
            rec4.addCell(cell4);
            otstable.insertRecord(rec4);

            //用行键（list）和RecordQueryOption查询
            RecordQueryOption recordQuery_listRK = new RecordQueryOption();
            recordQuery_listRK.setLimit(100L);
            List<byte[]> keys = new ArrayList<byte[]>();
            keys.add(0, rec3.getRowkey());
            keys.add(1, rec4.getRowkey());
            List<byte[]> bigkeys = new ArrayList<byte[]>();
            if (keys.size() > 0) {
                int count = 0;
                for (byte[] row : keys) {
                    bigkeys.add(count, otsAdmin.getBigRowkey(otstable.getId(), row));
                    count++;
                }
            }
            RecordResult r_listRK = otstable.getRecords(bigkeys,recordQuery_listRK);

            List<RowRecord> ff3 = r_listRK.getListRecords();
            for(int i = 0;i < ff3.size();i++){
                System.out.println("Listkey_lists:" + ff3.get(i).toString());
            }

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        }


    }


    /**
     * 更新记录
     */
    @Test
    public void updateRecordTest(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            RowRecord rec1 = new RowRecord();
            rec1.setRowkey("10000".getBytes());
            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
            rec1.addCell(cell1);

            //更改记录
            //修改原有记录
            cell1.setValue("test1_2".getBytes());
            //会增加一条rowkey相同但列名不同的记录
//            cell1.setName("col1_2".getBytes());
            otstable.updateRecord(rec1);
            System.out.println("Update the Record." );
        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除记录
     */
    @Test
    public void deleteAllRecordAllTest(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            //插入记录
            RowRecord rec1 = new RowRecord();
            rec1.setRowkey("10000".getBytes());
            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
            rec1.addCell(cell1);
            otstable.insertRecord(rec1);

            RowRecord rec2 = new RowRecord();
            rec2.setRowkey("20000".getBytes());
            RowCell cell2 = new RowCell("col2".getBytes(), "test2".getBytes());
            rec2.addCell(cell2);
            otstable.insertRecord(rec2);

            //设置记录
            RecordQueryOption recordQuery = new RecordQueryOption();
            recordQuery.setCursor_mark("*");
            recordQuery.setLimit(100L);
            //delete记录，全删
            otstable.deleteRecords(recordQuery);
            System.out.println("Delete records");

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除记录
     */
    @Test
    public void deleteRecord_RangeKeys_Test(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            //插入记录
            RowRecord rec1 = new RowRecord();
            rec1.setRowkey("10000".getBytes());
            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
            rec1.addCell(cell1);
            otstable.insertRecord(rec1);

            RowRecord rec2 = new RowRecord();
            rec2.setRowkey("20000".getBytes());
            RowCell cell2 = new RowCell("col2".getBytes(), "test2".getBytes());
            rec2.addCell(cell2);
            otstable.insertRecord(rec2);

            //设置记录
            RecordQueryOption recordQuery_rangeRK = new RecordQueryOption();
            recordQuery_rangeRK.setLimit(100L);
            byte[] startKey = otsAdmin.getBigRowkey(otstable.getId(),rec1.getRowkey());
            byte[] endKey = otsAdmin.getBigRowkey(otstable.getId(),rec2.getRowkey());

            //delete记录（行键range）,需要设置filter
            // 这里filter用正则表达式来匹配rowkey
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*10000"));
            recordQuery_rangeRK.setFilter(filter);

            otstable.deleteRecords(startKey, endKey, recordQuery_rangeRK);
            System.out.println("Delete records from startKey to endKey.");

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除记录
     */
    @Test
    public void deleteRecord_ListKeys_Test(){
        try {
            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls");
            //插入记录
            RowRecord rec3 = new RowRecord();
            rec3.setRowkey("30000".getBytes());
            RowCell cell3 = new RowCell("col3".getBytes(), "test3".getBytes());
            rec3.addCell(cell3);
            otstable.insertRecord(rec3);

            RowRecord rec4 = new RowRecord();
            rec4.setRowkey("40000".getBytes());
            RowCell cell4 = new RowCell("col4".getBytes(), "test4".getBytes());
            rec4.addCell(cell4);
            otstable.insertRecord(rec4);

            //设置记录
            RecordQueryOption recordQuery_listRK = new RecordQueryOption();
            recordQuery_listRK.setLimit(100L);
            List<byte[]> keys = new ArrayList<byte[]>();
            keys.add(0, rec3.getRowkey());
            keys.add(1, rec4.getRowkey());
            List<byte[]> bigkeys = new ArrayList<byte[]>();
            if (keys.size() > 0) {
                int count = 0;
                for (byte[] row : keys) {
                    bigkeys.add(count, otsAdmin.getBigRowkey(otstable.getId(), row));
                    count++;
                }
            }
            //delete记录（listKeys）
            otstable.deleteRecords(bigkeys);
            System.out.println("Delete records using the listkeys.");

        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (TableException e) {
            e.printStackTrace();
        } catch (OtsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


//    @Test
//    public void getRecordsTest(){
//        try {
//            OtsTable otstable = otsAdmin.getTable(12345, 101, "test_wls1");
//
//            //todo
//
//            otstable.getRecords(rec1);
//        } catch (ConfigException e) {
//            e.printStackTrace();
//        }
//
//        assert
//
//    }

    @After
    public void release(){
        otsAdmin.finalize();
        System.out.println("Junit is over.");
    }



}
