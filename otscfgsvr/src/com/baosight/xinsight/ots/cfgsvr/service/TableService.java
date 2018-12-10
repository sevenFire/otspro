package com.baosight.xinsight.ots.cfgsvr.service;

import com.baosight.xinsight.common.AasPermissionException;
import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.ConnException;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.model.operate.TableBackupModel;
import com.baosight.xinsight.ots.cfgsvr.util.BackupTask;
import com.baosight.xinsight.ots.cfgsvr.util.FTPUtil;
import com.baosight.xinsight.ots.cfgsvr.util.ThreadUtil;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.log4j.Logger;

import java.io.IOException;


public class TableService {
    private static final Logger LOG = Logger.getLogger(TableService.class);

    /**
     * 检查表是否存在
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static boolean exist(long userid, long tenantid, String tablename) throws OtsException {
        try {
            return ConfigUtil.getInstance().getOtsAdmin().isTableExist(userid, tenantid, tablename);
        } catch (MasterNotRunningException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
                    "Failed to check table exist or not because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
                    "Failed to check table exist or not because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (IOException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST,
                    "Failed to check table exist or not!\n" + e.getMessage());
        } catch (Exception e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST,
                    "Failed to check table exist or not!\n" + e.getMessage());
        }
    }


    /**
     * 导出表数据
     *
     * @return 0-all successful,other-表示不同的操作结果
     * @throws ConfigException
     * @throws TableException
     * @throws ConnException
     * @throws ClassNotFoundException
     */
    public static ErrorMode backupTable(long userid, long tenantid, String tablename, TableBackupModel backupModel) throws OtsException, ConnException, ClassNotFoundException {
        ErrorMode rMod = new ErrorMode(0L);
        try {
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userid, tenantid, tablename);
            if (table == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table not exist!");
            }
            String rediskeyTablename = getRedisKeyTableName(tenantid, table.getId(), tablename);
            String rediskeyTenantId = getRedisKeyTenantId(tenantid);
            boolean conn = testConnect(backupModel);
            if (!conn) {
                throw new ConnException("remote address connect refused.");
            }
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_SUCCESS);
            BackupTask backup = new BackupTask(RestConstants.DEFAULT_BACKUP_TYPE_BACKUP, table, backupModel);
            ThreadUtil.submit(backup);

            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_WAITING);
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_WAITING);
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "0");
        } catch (OtsException e) {
            e.printStackTrace();
            throw e;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new ConnException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ConnException(e);
        }
        LOG.debug("table backup submit complete.");
        return rMod;
    }

    public static ErrorMode restoreTable(long userId, long tenantId, String tenant, String userName, TableBackupModel model) throws OtsException, ConnException, ClassNotFoundException {
        ErrorMode rMod = new ErrorMode(0L);
        try {
            if (tenant != null) {
                PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
                userInfo.setTenantName(tenant);
                userInfo.setUserName(userName);
                userInfo.setTenantId(tenantId);
                userInfo.setUserId(userId);
                userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
                AasPermissionUtil.Permission permission = AasPermissionUtil.getPermissionByResource(
                        ConfigUtil.getInstance().getAuthServerAddr(), userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
                if (null == permission || !permission.isPost()) {
                    rMod.setError_code(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
                    rMod.setErrinfo("no permission to create table!");
                    LOG.info("no permission to create table! Error Code: (" + OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT + ")");
                    return rMod;
                }
            }
            boolean conn = testConnect(model);
            if (!conn) {
                throw new ConnException("remote address connect refused.");
            }
            OtsTable tableNew = ConfigUtil.getInstance().getOtsAdmin().getTable(userId, tenantId, model.getFileName());
            if (tableNew == null) {
                LOG.debug("target table " + model.getFileName() + " not exits.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "target table " + model.getFileName() + " not exits.");
            }
            String rediskeyTablename = getRedisKeyTableName(tenantId, tableNew.getId(), tableNew.getName());
            String rediskeyTenantId = getRedisKeyTenantId(tenantId);
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_SUCCESS);
            BackupTask restore = new BackupTask(RestConstants.DEFAULT_BACKUP_TYPE_RESTORE, tableNew, model);
            ThreadUtil.submit(restore);

            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_WAITING);
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_WAITING);
            ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "0");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new ConnException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ConnException(e);
        } catch (AasPermissionException e) {
            e.printStackTrace();
            throw new OtsException(e.getErrorCode(), e.getMessage());
        }
        LOG.debug("table restore submit complete.");
        return rMod;
    }

    private static boolean testConnect(TableBackupModel model) throws IOException, OtsException {
        boolean flag = false;
        long errcode = 0L;

        if (model.getMode() == RestConstants.DEFAULT_CONNECT_TYPE_FTP) {
            FTPUtil ftp = new FTPUtil(model.getHost(), model.getPort(), model.getUsername(), model.getPassword());
            errcode = ftp.test(model.getDst());
            if (errcode != 0) {
                throw new OtsException(errcode, "Connect refused.");
            } else {
                flag = true;
            }
        }

        return flag;
    }

    public static String getRedisKeyTableName(long tenantid, long tableid, String tablename) {
        StringBuilder sb = new StringBuilder();
        sb.append(RestConstants.DEFAULT_BACKUP_PREFIX).append(tableid).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT);
        sb.append(tenantid).append(":").append(tablename);
        return sb.toString();
    }

    public static String getRedisKeyTenantId(long tenantid) {
        StringBuilder sb = new StringBuilder();
        sb.append(RestConstants.DEFAULT_BACKUP_PREFIX).append(tenantid);
        return sb.toString();
    }
}
