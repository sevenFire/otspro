package com.baosight.xinsight.ots.rest.service;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.MessageHandlerFactory;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.model.UserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.body.table.TableCreateBodyModel;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.model.TableInfoListModel;
import com.baosight.xinsight.ots.rest.model.TableInfoModel;
import com.baosight.xinsight.ots.rest.model.TableListModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.TableCreateModel;
import com.baosight.xinsight.ots.rest.model.operate.TableUpdateModel;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.MessageBuilder;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.TableConfigUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.log4j.Logger;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class TableService {
    private static final Logger LOG = Logger.getLogger(TableService.class);

    public static Integer convertCompression(String compressName) {
        if (compressName == null) {
            return null;
        }

        if (compressName.equalsIgnoreCase(Algorithm.LZO.getName())) {
            return OtsConstants.TABLE_ALG_LZO;
        } else if (compressName.equalsIgnoreCase(Algorithm.GZ.getName())) {
            return OtsConstants.TABLE_ALG_GZ;
        } else if (compressName.equalsIgnoreCase(Algorithm.NONE.getName())) {
            return OtsConstants.TABLE_ALG_NONE;
        } else if (compressName.equalsIgnoreCase(Algorithm.SNAPPY.getName())) {
            return OtsConstants.TABLE_ALG_SNAPPY;
        } else if (compressName.equalsIgnoreCase(Algorithm.LZ4.getName())) {
            return OtsConstants.TABLE_ALG_LZ4;
        } else {
            return null;
        }
    }

    public static String convertCompression(Integer compressType) {
        if (compressType == null) {
            return null;
        }

        if (compressType == OtsConstants.TABLE_ALG_LZO) {
            return Algorithm.LZO.getName();
        } else if (compressType == OtsConstants.TABLE_ALG_GZ) {
            return Algorithm.GZ.getName();
        } else if (compressType == OtsConstants.TABLE_ALG_NONE) {
            return Algorithm.NONE.getName();
        } else if (compressType == OtsConstants.TABLE_ALG_SNAPPY) {
            return Algorithm.SNAPPY.getName();
        } else if (compressType == OtsConstants.TABLE_ALG_LZ4) {
            return Algorithm.LZ4.getName();
        } else {
            return null;
        }
    }

    public static TableListModel getTablelist(PermissionCheckUserInfo userInfo) throws Exception {
        TableListModel tableList = new TableListModel();
        try {
            List<Long> noGetPermissionList = null;
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                List<OtsTable> permittedTables = ConfigUtil.getInstance().getOtsAdmin().getPermisstionTables(userInfo.getUserId(), userInfo.getTenantId());
                List<Long> byPermittedObjects = new ArrayList<Long>();
                for (OtsTable table : permittedTables) {
                    byPermittedObjects.add(table.getId());
                }
                noGetPermissionList = AasPermissionUtil.obtainNoGetPermissionInstanceList(ConfigUtil.getInstance().getAuthServerAddr(), userInfo, byPermittedObjects);
            }

            List<OtsTable> lstTables = ConfigUtil.getInstance().getOtsAdmin().getAllTables(userInfo.getUserId(), userInfo.getTenantId(), noGetPermissionList);
            tableList.setTotalcount((long) lstTables.size());
            tableList.setErrcode(0L);
            for (OtsTable table : lstTables) {
                String tablename = table.getName();
                tableList.add(tablename);
            }

            TableConfigUtil.cacheTableConfig(userInfo.getUserId(), userInfo.getTenantId(), lstTables);
        } catch (ConfigException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            throw e;
        }

        LOG.debug("RETURN:" + tableList.toString());
        return tableList;
    }

    public static TableListModel getTablelist(PermissionCheckUserInfo userInfo, String name, Long limit, Long offset) throws Exception {
        TableListModel tableList = new TableListModel();

        try {
            List<Long> noGetPermissionList = null;
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                List<OtsTable> permittedTables = ConfigUtil.getInstance().getOtsAdmin().getPermisstionTables(userInfo.getUserId(), userInfo.getTenantId());
                List<Long> byPermittedObjects = new ArrayList<Long>();
                for (OtsTable table : permittedTables) {
                    byPermittedObjects.add(table.getId());
                }
                noGetPermissionList = AasPermissionUtil.obtainNoGetPermissionInstanceList(ConfigUtil.getInstance().getAuthServerAddr(), userInfo, byPermittedObjects);
            }
            List<OtsTable> lstTables = ConfigUtil.getInstance().getOtsAdmin().getAllTables(userInfo.getUserId(), userInfo.getTenantId(), name, limit, offset, noGetPermissionList);
            for (OtsTable table : lstTables) {
                String tablename = table.getName();
                tableList.add(tablename);
            }
            tableList.setTotalcount(ConfigUtil.getInstance().getOtsAdmin().countAllTables(userInfo.getUserId(), userInfo.getTenantId(), name, noGetPermissionList));
            tableList.setErrcode(0L);

            TableConfigUtil.cacheTableConfig(userInfo.getUserId(), userInfo.getTenantId(), lstTables);
        } catch (ConfigException e) {
            throw e;
        }

        LOG.debug("RETURN:" + tableList.toString());
        return tableList;
    }

    /**
     * get table info
     *
     * @throws Exception
     */
    public static TableInfoModel getTableInfo(PermissionCheckUserInfo userInfo, String tableName) throws ConfigException, OtsException, IOException, Exception {
        TableInfoModel model = new TableInfoModel(tableName);

        try {
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (table == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, String.format("user (id:%d) was not owned table:%s!", userInfo.getUserId(), tableName));
            }
            long tableId = table.getId();
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.CacheInfo cacheInfo = PermissionUtil.GetInstance().otsPermissionHandler(userInfo, tableId, PermissionUtil.PermissionOpesration.READ);
                MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC).sendData(userInfo.getTenantId(),
                        MessageBuilder.buildPermissionMessage(userInfo, table.getId(), cacheInfo.isReadPermission(), cacheInfo.isWritePermission(), cacheInfo.isPermissionFlag(), cacheInfo.getCurrentTime()));
            }

            model.setDescription(table.getDescription());
            model.setMaxVersions(table.getMaxversion());
            model.setCompressionType(convertCompression(table.getCompression()));
            model.setEnable(table.getEnable());
            model.setMobEnabled(table.getMobEnabled());
            model.setMobThreshold(table.getMobThreshold());

            SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 时间格式化的格式
            model.setCreateTime(sDateFormat.format(table.getCreateTime()));
            model.setLastModify(sDateFormat.format(table.getModifyTime()));
            model.setKeyType(table.getKeyType());
            model.setHashKeyType(table.getHashKeyType());
            model.setRangeKeyType(table.getRangeKeyType());
            model.setErrcode(0L);
            model.setId(tableId);
            // update cache
            TableConfigUtil.addTableConfig(userInfo.getUserId(), userInfo.getTenantId(), model);
        } catch (ConfigException e) {
            throw e;
        } catch (OtsException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        }

        LOG.debug("RETURN:" + model.toString());
        return model;
    }

    /**
     * get all tables information
     *
     * @param userInfo
     * @return List<TableInfoModel>
     * @throws SQLException
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static TableInfoListModel getAllTablesInfo(PermissionCheckUserInfo userInfo) throws ConfigException, IOException {
        TableInfoListModel listModel = new TableInfoListModel();
        try {
            List<Long> noGetPermissionList = null;
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                // 过滤出id list
                List<OtsTable> permittedTables = ConfigUtil.getInstance().getOtsAdmin().getPermisstionTables(userInfo.getUserId(), userInfo.getTenantId());
                List<Long> byPermittedObjects = new ArrayList<Long>();
                for (OtsTable table : permittedTables) {
                    byPermittedObjects.add(table.getId());
                }
                // 再调用AasPermissionUtil接口获取ConfigUtil.getInstance().getAuthServerAddr()，noGetPermissionList
                try {
                    noGetPermissionList = AasPermissionUtil.obtainNoGetPermissionInstanceList(ConfigUtil.getInstance().getAuthServerAddr(), userInfo, byPermittedObjects);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                }
            }
            List<OtsTable> lstTables = ConfigUtil.getInstance().getOtsAdmin().getAllTables(userInfo.getUserId(), userInfo.getTenantId(), noGetPermissionList);
            for (OtsTable table : lstTables) {
                TableInfoModel model = new TableInfoModel(table.getName());
                model.setDescription(table.getDescription());
                model.setMaxVersions(table.getMaxversion());
                model.setCompressionType(convertCompression(table.getCompression()));
                model.setEnable(table.getEnable());
                model.setMobEnabled(table.getMobEnabled());
                model.setMobThreshold(table.getMobThreshold());

                SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                model.setCreateTime(sDateFormat.format(table.getCreateTime()));
                model.setLastModify(sDateFormat.format(table.getModifyTime()));
                model.setKeyType(table.getKeyType());
                model.setHashKeyType(table.getHashKeyType());
                model.setRangeKeyType(table.getRangeKeyType());
                model.setId(table.getId());

                listModel.add(model);
                // update cache
                TableConfigUtil.addTableConfig(userInfo.getUserId(), userInfo.getTenantId(), model);
            }
        } catch (ConfigException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
        listModel.setErrcode(0L);
        listModel.setCount(listModel.getTableinfolist().size());

        LOG.debug("RETURN:" + listModel.toString());
        return listModel;
    }

    /**
     * check table exist or not
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static boolean exist(UserInfo userInfo, String tableName) throws OtsException {
        try {
            return ConfigUtil.getInstance().getOtsAdmin().isTableExist(userInfo.getUserId(), tableName);
        } catch (MasterNotRunningException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to check table exist or not because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to check table exist or not because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (IOException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        } catch (Exception e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        }
    }

    /**
     * check table exist or not
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static boolean innerHbaseTableExist(long userid, long tenantid, String tablename) throws OtsException {
        try {
//            return ConfigUtil.getInstance().getOtsAdmin().isHbaseTableExist(userid, tenantid, tablename);
            // todo 改成返回大表的
            return ConfigUtil.getInstance().getOtsAdmin().isHbaseTableExist(tenantid);
        } catch (MasterNotRunningException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to check table exist or not because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to check table exist or not because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (IOException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        } catch (Exception e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        }
    }
    
    /**
     * check table exist or not
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static boolean innerHbaseCreateTable(long userid, long tenantid, String tablename, TableCreateModel model) throws OtsException {
        try {
//            ConfigUtil.getInstance().getOtsAdmin().createHbaseTable(tenantid, tablename,
//            		model.hasCompressionType() ? convertCompression(model.getCompressionType()) : Algorithm.NONE.getName(),
//            		model.getMaxVersions(), model.getMobEnabled(), model.getMobThreshold(), model.hasReplication());


            ConfigUtil.getInstance().getOtsAdmin().createHaseTable(tenantid,
                    model.hasCompressionType() ? convertCompression(model.getCompressionType()) : Algorithm.NONE.getName(),
                    model.getMaxVersions(), model.getMobEnabled(), model.getMobThreshold(), model.hasReplication());
        } catch (OtsException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        } catch (Exception e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Failed to check table exist or not!\n" + e.getMessage());
        }
        
        return true;
    }

    /**
     * delete table
     *
     * @throws IndexException
     * @throws TableException
     * @throws Exception
     */
    public static ErrorMode deleteTable(PermissionCheckUserInfo userInfo, String tableName) throws ConfigException, IndexException, OtsException, TableException {
        LOG.debug("deleteTable begin: uid=" + userInfo.getUserId() + ", namespace=" + userInfo.getTenantId() + ", tablename=" + tableName);
        ErrorMode rMod = new ErrorMode(0L);
        try {
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, -1, PermissionUtil.PermissionOpesration.OTSMANAGEW);
                // remove relevant permission cache of current table
                CachePermission.getInstance().batchRemove((new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY)
                        .append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(table.getId()).toString());
                // Kafka produces permission remove message
                MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC)
                        .sendData(userInfo.getTenantId(), MessageBuilder.buildPermissionRemoveMessage((new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY)
                                .append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(table.getId()).toString()));
            }

            if (table != null) {
                ConfigUtil.getInstance().getOtsAdmin().deleteTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            }

            // delete metrics in redis
            ConfigUtil.getInstance().getRedisUtil().delHSet(RestConstants.DEFAULT_METRICS_PREFIX + userInfo.getTenantId(), tableName);

            // delete backup state in redis
            ConfigUtil.getInstance().getRedisUtil().del(getRedisKeyTableName(userInfo.getTenantId(), table.getId(), tableName));

            //delete cache
            TableConfigUtil.deleteTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);

            //Kafka produces config message
            MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC)
                    .sendData(userInfo.getTenantId(), MessageBuilder.buildConfigMessage(1, userInfo.getTenantId(), userInfo.getUserId(), tableName));

            LOG.debug("deleteTable successful: uid=" + userInfo.getUserId() + ", namespace=" + userInfo.getUserId() + ", tablename=" + tableName);

        } catch (ConfigException e) {
            throw e;
        } catch (IndexException e) {
            throw e;
        } catch (MasterNotRunningException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to delete table because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to delete table because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (IOException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_DELETE, "Failed to delete table!\n" + e.getMessage());
        } catch (TableException e) {
            throw e;
        } catch (OtsException e) {
            throw e;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_DELETE, "Failed to delete table!\n" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_DELETE, "Failed to delete table!\n" + e.getMessage());
        }
        return rMod;
    }

    /**
     * create new table
     *
     * @return created table
     * @throws Exception
     */
    public static OtsTable createTable(PermissionCheckUserInfo userInfo, String tableName, TableCreateBodyModel createBodyModel) throws Exception {
        try {
            //存入userInfo到缓存
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.CacheInfo info = PermissionUtil.GetInstance().otsPermissionHandler(userInfo, -1, PermissionUtil.PermissionOpesration.OTSMANAGEW);
                MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC)
                        .sendData(userInfo.getTenantId(), MessageBuilder.buildPermissionMessage(userInfo, RestConstants.OTS_MANAGE_PERMISSION_OPERATION, 
                                info.isReadPermission(), info.isWritePermission(), info.isPermissionFlag(), info.getCurrentTime()));
            }

//            //判定pg中小表是否已经存在
              //在create方法中已经校验了。
//            if (TableService.exist(userInfo, tableName)) {
//                LOG.error(Response.Status.CONFLICT.name() + ":" + tableName + " has exist.");
//                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_EXIST, Response.Status.CONFLICT.name() + ":" + tableName + " has exist.");
//            }

            //创建表（包含大表和小表），以及表存在性校验
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().createTable(userInfo.getUserId(), userInfo.getTenantId(), tableName,
                    createBodyModel.toTable());

            //add cache
            TableInfoModel info = new TableInfoModel(table.getName());
            info.setKeyType(table.getKeyType());
            info.setHashKeyType(table.getHashKeyType());
            info.setRangeKeyType(table.getRangeKeyType());
            TableConfigUtil.addTableConfig(userInfo.getUserId(), userInfo.getTenantId(), info);

            //Kafka produces config message
            MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC).sendData(userInfo.getTenantId(), MessageBuilder.buildConfigMessage(0, userInfo.getTenantId(), userInfo.getUserId(), tableName));

            return table;
        } catch (TableException e) {
            throw e;
        } catch (ConfigException e) {
            throw e;
        } catch (OtsException e) {
            throw e;
        }
    }

    /**
     * update table
     *
     * @return 0-all successful,other- had error
     * @throws Exception
     */
    public static ErrorMode updateTable(PermissionCheckUserInfo userInfo, String tableName, TableUpdateModel model) throws Exception {
        ErrorMode rmodel = new ErrorMode(0L);

        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.READ);
            }
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (table == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table not exist!");
            }
            if (!model.isValid(table.getMobEnabled())) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE, "invalid table scheme!");
            }
            table.updateTable(model.getDescription(), model.getMaxVersions(), model.getEnable(), model.getMobEnabled(), model.getMobThreshold(), model.getReplication());
        } catch (TableException e) {
            LOG.error("Failed to update table info for!\n" + e.getMessage());
            rmodel.setError_code(e.getErrorCode());
            rmodel.setErrinfo(e.getMessage());
        } catch (OtsException e) {
            LOG.error("Failed to update table info for!\n" + e.getMessage());
            rmodel.setError_code(e.getErrorCode());
            rmodel.setErrinfo(e.getMessage());
        } catch (IOException e) {
            LOG.error("Failed to update table info for!\n" + e.getMessage());
            rmodel.setError_code(OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE);
            rmodel.setErrinfo(e.getMessage());
        } catch (Exception e) {
            throw e;
        }
        LOG.debug("RETURN:" + rmodel.toString());
        return rmodel;
    }

    // ///////////////////////

    /**
     * get table configuration
     *
     * @param userid
     * @param tablename
     * @return
     */
    public static String getConfig(long userid, long tenantid, String tablename) throws ConfigException {
        try {
            return ConfigUtil.getInstance().getOtsAdmin().getTableConfig(userid, tenantid, tablename);
        } catch (ConfigException e) {
            throw e;
        }
    }

    /**
     * save table configuration
     *
     * @param userid
     * @param tablename
     * @param config
     * @return
     * @throws ConfigException
     */
    public static void saveConfig(long userid, long tenantid, String tablename, String config) throws ConfigException {
        try {
            ConfigUtil.getInstance().getOtsAdmin().saveTableConfig(userid, tenantid, tablename, config);
        } catch (ConfigException e) {
            throw e;
        }
    }

    private static String getRedisKeyTableName(long tenantid, long tableid, String tablename) {
        StringBuilder sb = new StringBuilder();
        sb.append(RestConstants.DEFAULT_BACKUP_PREFIX).append(tableid).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT);
        sb.append(tenantid).append(":").append(tablename);
        return sb.toString();
    }
}
