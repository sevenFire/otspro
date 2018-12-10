package com.baosight.xinsight.ots.cfgsvr.bean;

import com.baosight.xinsight.common.AasPermissionException;
import com.baosight.xinsight.common.CommonErrorCode;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.exception.PermissionSqlException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


//import com.baosight.xinsight.ots.common.util.AasPermissionUtil;

public class AuthBean {
    private static final Logger LOG = Logger.getLogger(AuthBean.class);
    private static Map<String, Boolean> mapIsRegisted = new HashMap<String, Boolean>();

    private boolean hasRecordWritePerm;
    private boolean hasManagePerm;
    private boolean hasAuthPerm;
    private Boolean permissionFlag = false;
    private String token = "";
    private String tenant = "";
    private String username = "";
    /**
     * user properties tenantId is not exist
     */
    private String tenantId = "-1";
    /**
     * user properties userId is not exist
     */
    private String userId = "-1";
    private String serviceName = "";
    /**
     * ots_user_table not exist
     */
    private String tableId = "-2";


    public AuthBean() {
    }

    /**
     * Attention: service name is OTS, serviceName param must be Caps
     *
     * directly check current user whether or not has OTSTABLE_tableID ,
     *   GET: permission for viewing OTS and relevant data and index
     *   POST,PUT ,DELETE: permission for creating data and index
     * @return
     */
    public boolean isHasRecordWritePerm() {
        hasRecordWritePerm = false;
        AasPermissionUtil.Permission permission = null;
        try {
            if (null == tableId || tableId.isEmpty()) {
                LOG.error("Not get the tableID in AuthBean, thus record write permission forces as False !");
            } else {
                PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
                userInfo.setTenantName(tenant);
                userInfo.setUserName(username);
                userInfo.setTenantId(Long.parseLong(tenantId));
                userInfo.setUserId(Long.parseLong(userId));
                userInfo.setServiceName(serviceName);
                permission = AasPermissionUtil.getInstancePermission(ConfigUtil.getInstance().getAuthServerAddr(), userInfo, Long.valueOf(tableId));
                permissionFlag = ConfigUtil.getInstance().getOtsAdmin().checkTablePermitted(Long.valueOf(tableId));
                if (null == permission) {
                    LOG.error(String.format("OTS Obtain write permission exception, reason is token expire or token not exist "));
                    throw new NullPointerException();
                }
                if ((permission != null && (permission.isPost() || (permissionFlag == false && !permission.isPost())))) {
                    hasRecordWritePerm = true;
                    LOG.debug("OTS manage Bean Action Obtained Records Write Permission:" + String.valueOf(hasRecordWritePerm));
                }
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (PermissionSqlException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } finally {

        }
        return hasRecordWritePerm;
    }

    public void setHasRecordWritePerm(boolean hasRecordWritePerm) {
        this.hasRecordWritePerm = hasRecordWritePerm;
    }

    /**
     * directly check current user whether or not has ots_manage POST , PUT ,DELETE permission for creating new OTS
     * table and relevant data and index.
     * @return
     */
    public boolean isHasManagePerm() {
        hasManagePerm = false;
        AasPermissionUtil.Permission permission = null;
        try {
            /**
             * ots_manage resource registed by jps at ots table_list.html login.
             */
            registOTSInstance();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo.setTenantName(tenant);
            userInfo.setUserName(username);
            userInfo.setTenantId(Long.parseLong(tenantId));
            userInfo.setUserId(Long.parseLong(userId));
            userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
            permission = AasPermissionUtil.getPermissionByResource(ConfigUtil.getInstance().getAuthServerAddr()
                    , userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
            if (null == permission) {
                LOG.error(String.format("OTS Obtain manage permission exception, reason is token expire or token not exist "));
                throw new NullPointerException();
            }
            if (permission != null && permission.isPost()) {
                hasManagePerm = true;
                LOG.debug("OTS manage Bean Action Obtained POST Permission:" + String.valueOf(permission.isPost()));
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (AasPermissionException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
        return hasManagePerm;
    }

    public void setHasManagePerm(boolean hasManagePerm) {
        this.hasManagePerm = hasManagePerm;
    }

    /**
     * directly check current user whether or not has ots_manage GET permission for viewing OTS
     * table and relevant data and index.
     * @return
     */
    public boolean isHasAuthPerm() {
        hasAuthPerm = false;
        AasPermissionUtil.Permission permission = null;
        try {
            /**
             * ots_manage resource registed by jps at ots table_list.html login.
             */
            registOTSInstance();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo.setTenantName(tenant);
            userInfo.setUserName(username);
            userInfo.setTenantId(Long.parseLong(tenantId));
            userInfo.setUserId(Long.parseLong(userId));
            userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
            permission = AasPermissionUtil.getPermissionByResource(ConfigUtil.getInstance().getAuthServerAddr()
                    , userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
            if (null == permission) {
                LOG.error(String.format("OTS Obtain read permission exception, reason is token expire or token not exist "));
                throw new NullPointerException();
            }
            if (permission != null && permission.isGet()) {
                hasAuthPerm = true;
                LOG.debug("OTS manage Bean Action Obtained GET Permission:" + String.valueOf(permission.isGet()));
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (AasPermissionException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
        return hasAuthPerm;
    }

    public void setHasAuthPerm(boolean hasAuthPerm) {
        this.hasAuthPerm = hasAuthPerm;
    }

    public void registOTSInstance() {
        try {
            if (!mapIsRegisted.containsKey(tenantId) || !mapIsRegisted.get(tenantId)) {
                LOG.debug(String.format("[%s] begin to register 'ots_manage' resource into db, username[%s] tenant[%s]"
                        ,OtsConstants.OTS_SERVICE_NAME  ,username  ,tenant));
                PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
                userInfo.setTenantName(tenant);
                userInfo.setUserName(username);
                userInfo.setTenantId(Long.parseLong(tenantId));
                userInfo.setUserId(Long.parseLong(userId));
                userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
                long errcode = AasPermissionUtil.registerResourceManageResource(
                        ConfigUtil.getInstance().getAuthServerAddr(), userInfo);
                if (errcode == 0 || errcode == CommonErrorCode.EC_COMMON_AAS_RESOURCE_DUP) {
                    mapIsRegisted.put(tenantId, true);
                    LOG.info(String.format("otscfgsvr ots_manage resource map has been updated, resource map info:[%s]"
                            , mapIsRegisted.toString()));
                } else {
                    mapIsRegisted.put(tenantId, false);
                    LOG.error("****The Ots Manage Instance Addition failed!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
