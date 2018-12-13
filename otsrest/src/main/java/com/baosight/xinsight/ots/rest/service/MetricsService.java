package com.baosight.xinsight.ots.rest.service;

import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.OTSTable;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.common.table.TableMetrics;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.model.MetricsListModel;
import com.baosight.xinsight.ots.rest.model.MetricsModel;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MetricsService {
    private static final Logger LOG = Logger.getLogger(MetricsService.class);

    public static void getMetricsInfoByNamespaceFromHbase(long tenantid) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, TableException, OtsException, ConfigException {
        Map<String, TableMetrics> metricsMap = ConfigUtil.getInstance().getOtsAdmin().getTableMetrics(tenantid);
        for (String tablename : metricsMap.keySet()) {
            TableMetrics metrics = metricsMap.get(tablename);
            String tableFullName = String.valueOf(tenantid) + TableName.NAMESPACE_DELIM + tablename;
            ConfigUtil.getInstance().getRedisUtil().setHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_READ_COUNT, String.valueOf(metrics.getlReadRequestsCount()));
            ConfigUtil.getInstance().getRedisUtil().setHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_WRITE_COUNT, String.valueOf(metrics.getlWriteRequestsCount()));
            ConfigUtil.getInstance().getRedisUtil().setHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_DISK_SIZE, String.valueOf(metrics.getlStorefileSize()));
            ConfigUtil.getInstance().getRedisUtil().setHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_REGION_COUNT, String.valueOf(metrics.getlRegionCount()));
        }
    }

    /**
     * get table rowcount
     *
     * @param zookeeperQuorum host1:port,host2:port
     * @param redisQuorum     host1:port,host2:port
     * @param redisPass       redis password if auth is opened
     * @param fulltablename   the full table name(with namespace)
     * @return row count, 0 if error
     * @throws IOException
     * @throws TableException
     */
    private static void getTableRowCount(String zookeeperQuorum, String redisQuorum, String redisPass, String fulltablename) throws IOException, TableException {
        try {
            System.out.println("getTableRowCount for " + fulltablename);
            String cmd = null;
            if (null == redisPass || redisPass.length() == 0) {
                cmd = "nohup hadoop jar " + OtsConstants.OTS_MAPREDUCE_JAR_PATH + " com.baosight.xinsight.ots.mapreduce.table_row_counter.TableRowCounter --zookeeper_quorum " + zookeeperQuorum + " --redis_quorum " + redisQuorum + " --table_name " + fulltablename;
            } else {
                cmd = "nohup hadoop jar " + OtsConstants.OTS_MAPREDUCE_JAR_PATH + " com.baosight.xinsight.ots.mapreduce.table_row_counter.TableRowCounter --zookeeper_quorum " + zookeeperQuorum + " --redis_quorum " + redisQuorum + "--redis_pass " + redisPass + " --table_name " + fulltablename;
            }
            String[] cmds = new String[]{"/bin/sh", "-c", cmd};
            Process ps = Runtime.getRuntime().exec(cmds);
            ps.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getRowCountByNamespaceFromHbase(long tenantid) throws TableException, IOException, OtsException, ConfigException {
        Map<String, TableMetrics> metricsMap = ConfigUtil.getInstance().getOtsAdmin().getTableMetrics(tenantid);
        for (String tablename : metricsMap.keySet()) {
            String tableFullName = String.valueOf(tenantid) + TableName.NAMESPACE_DELIM + tablename;
            if (ConfigUtil.getInstance().getOtsAdmin().isTableEnabled(tenantid, tablename)) {
                getTableRowCount(ConfigUtil.getInstance().getValue(ConfigConstants.ZOOKEEPER_QUORUM), ConfigUtil.getInstance().getValue(ConfigConstants.REDIS_QUORUM), ConfigUtil.getInstance().getValue(ConfigConstants.REDIS_PASSWORD), tableFullName);
            } else {
                LOG.warn("Table: " + tableFullName + " is disabled, just suppose that its record count to be 0.");
            }
        }
    }

    public static MetricsListModel getMetricsInfoByNamespace(long tenantid, long userid) throws OtsException {

        MetricsListModel modellist = new MetricsListModel();
        try {
            List<Long> noGetPermissionList = null;
            List<OTSTable> listTables = ConfigUtil.getInstance().getOtsAdmin().getAllTablesByTid(tenantid, noGetPermissionList);
            for (OTSTable table : listTables) {
                MetricsModel model = getMetricsInfo(tenantid, table.getName());
                if (model != null) {
                    modellist.add(model);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(RestErrorCode.EC_OTS_REST_METRICS_NAMESPACE, String.format("Get namespace %d metrics info error!", tenantid));
        }
        modellist.setErrcode(0L);

        LOG.debug("RETURN:" + modellist.toString());
        return modellist;
    }

    public static MetricsModel getMetricsInfo(long tenantid, String tablename) throws OtsException {

        MetricsModel model = new MetricsModel();
        try {
            model.setTablename(tablename);
            String tableFullName = String.valueOf(tenantid) + TableName.NAMESPACE_DELIM + tablename;

            String value = ConfigUtil.getInstance().getRedisUtil().getHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_READ_COUNT);
            model.setReadRequestsCount(value != null ? Long.parseLong(value) : 0);

            value = ConfigUtil.getInstance().getRedisUtil().getHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_WRITE_COUNT);
            model.setWriteRequestsCount(value != null ? Long.parseLong(value) : 0);

            value = ConfigUtil.getInstance().getRedisUtil().getHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_TABLE_DISK_SIZE);
            model.setStorefileSize(value != null ? Long.parseLong(value) : 0);

            value = ConfigUtil.getInstance().getRedisUtil().getHSet(RestConstants.DEFAULT_METRICS_PREFIX + tableFullName, RestConstants.METRICS_REGION_COUNT);
            model.setRegionCount(value != null ? Long.parseLong(value) : 0);
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(RestErrorCode.EC_OTS_REST_METRICS_TABLE, String.format("Get table %s metrics info error!", tablename));
        }
        model.setErrcode(0L);

        LOG.debug("RETURN:" + model.toString());
        return model;
    }

    public static boolean isNamespaceExist(long tenantid) throws OtsException {
        try {
            return ConfigUtil.getInstance().getOtsAdmin().isNamespaceExist(tenantid);
        } catch (OtsException e) {
            throw e;
        }
    }
}
