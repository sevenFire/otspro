package com.baosight.xinsight.ots.rest.util;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.common.CommonErrorCode;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.permission.ScheduleThread;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;



/**
 * @author Zhang Yunlong
 * 当前类主要负责rest接口对于表以及表所属资源的权限校验
 * 表的查看权限， 表的增删改权限
 * 表所属资源增删改查权限
 * （资源的授权权限由otscfgsvr去实现校验）
 */
public class PermissionUtil {
    private static final Logger LOG = Logger.getLogger(PermissionUtil.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static PermissionUtil instance = null;
    private ScheduleThread singletonThread = null; // start the schedule Thread and keep it is only started once
    private static String curretnServicename = "";

    public static void init(String calledServiceName) {
		curretnServicename = calledServiceName;
	}
    
    private PermissionUtil() {
        singletonThread = new ScheduleThread(curretnServicename);
        Timer cacheManage = new Timer("Permission Cache Schedual Thread");
        try {
            String threadStartDelay = ConfigUtil.getInstance().getValue(ConfigConstants.OTS_PERMISSION_CHECK_THREAD_START_DELAY, "10000").trim();
            String threadCheckInterval = ConfigUtil.getInstance().getValue(ConfigConstants.OTS_PERMISSION_CHECK_THREAD_INTERVAL, "400000").trim();
            cacheManage.schedule(singletonThread, Long.valueOf(threadStartDelay), Long.valueOf(threadCheckInterval));
            LOG.info("begin to read the ots config >>>>>> ");
            LOG.info("********************************");
            LOG.info(String.format("Permission cache schedual thread start thread hashCode [%s] thread start delay [%s](ms) thread check interval [%s](ms)"
                    , singletonThread.hashCode(), threadStartDelay, threadCheckInterval));
            LOG.info("********************************");
            LOG.info("<<<<<< end to read the ots config ");
        } catch (NumberFormatException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }
    public static enum PermissionOpesration {
        READ,
        EDIT,
        OTSMANAGER,
        OTSMANAGEW
    }

    public static synchronized PermissionUtil GetInstance() {
        if (null == instance) {
            instance = new PermissionUtil();
        }
        return instance;
    }

    public synchronized void stop() {
        if (instance != null) {
            try {
                singletonThread.cancel();
                CachePermission.getInstance().getEhcacheManager().removeCache(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME);
                CachePermission.getInstance().getEhcacheManager().close();
            } catch (NumberFormatException e) {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
            } catch (IOException e) {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
            }
            instance = null;
        }
    }

    /**
     * 校验用户对表以及表所属资源（data, index）的查看权限
     * @param userInfo
     * @param tableId
     * @return CacheInfo
     * @throws OtsException
     */
    public synchronized CacheInfo checkReadPermission(PermissionCheckUserInfo userInfo, long tableId) throws OtsException {
        CacheInfo info = new CacheInfo();
        String useTime = sdf.format(new Date());
        CachePermission.CacheItem otsManagePermission;
        try {
            otsManagePermission = CachePermission.getInstance().getPermission(userInfo, RestConstants.OTS_MANAGE_PERMISSION_OPERATION, useTime);
            if (null == otsManagePermission) {
                LOG.error(String.format("Failed to obtain OTS Read Permission, InstanceId [%s] tenant[%s] username[%s]:", String.valueOf(tableId), userInfo.getTenantName(), userInfo.getUserName()));
                throw new OtsException(RestErrorCode.EC_OTS_REST_GET_CACEH_PERMISSION_EXCEPTION, "Obtained cache otsPermission failed !");
            }

            info.setReadPermission(otsManagePermission.isOtsGetPermission());
            info.setWritePermission(otsManagePermission.isOtsPostPermission());
            if (!otsManagePermission.isOtsPostPermission() && !otsManagePermission.isOtsGetPermission()) {
                CachePermission.CacheItem tablePermission = CachePermission.getInstance().getPermission(userInfo, tableId, useTime);
                if (null == tablePermission) {
                    LOG.error(String.format("Failed to obtain OTS Read Permission, InstanceId [%s] tenant[%s] username[%s]:", String.valueOf(tableId), userInfo.getTenantName(), userInfo.getUserName()));
                    throw new OtsException(RestErrorCode.EC_OTS_REST_GET_TABLE_PERMISSION_EXCEPTION, "Obtained cache tablePermission failed !");
                }

                info.setReadPermission(tablePermission.isOtsGetPermission());
                info.setWritePermission(tablePermission.isOtsPostPermission());
                info.setPermissionFlag(tablePermission.getPermissionFlag());
                if (tablePermission.getPermissionFlag()) {
                    if (!tablePermission.isOtsGetPermission()) {
                        LOG.error(String.format("Operation Failed Since No Read Permission, InstanceId [%s] tenant[%s] username[%s]:", String.valueOf(tableId), userInfo.getTenantName(), userInfo.getUserName()));
                        info.setErrorCode(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
                    }
                }
            }
        } catch (ConfigException | IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        }
        LOG.debug("Current uesr account owns table read permission");
        info.setCurrentTime(useTime);
        return info;
    }

    /**
     * 校验用户对表下资源（data, index）的增删改查得权限
     * @param userInfo
     * @param tableId
     * @return CacheInfo
     * @throws OtsException
     */
    public synchronized CacheInfo checkEditPermission(PermissionCheckUserInfo userInfo, long tableId) throws OtsException {
        CacheInfo info = new CacheInfo();
        String useTime = sdf.format(new Date());
        CachePermission.CacheItem otsManagePermission;
        try {
            /**
            * 获取ots_manage资源的权限
            */
            otsManagePermission = CachePermission.getInstance().getPermission(userInfo, RestConstants.OTS_MANAGE_PERMISSION_OPERATION, useTime);
            if (null == otsManagePermission) {
                throw new OtsException(RestErrorCode.EC_OTS_REST_GET_CACEH_PERMISSION_EXCEPTION, "Obtained cache otsPermission failed !");
            }
            info.setReadPermission(otsManagePermission.isOtsGetPermission());
            info.setWritePermission(otsManagePermission.isOtsPostPermission());
            if (!otsManagePermission.isOtsPostPermission()) {
                CachePermission.CacheItem tablePermission = CachePermission.getInstance().getPermission(userInfo, tableId, useTime);
                if (null == tablePermission) {
                    LOG.error(String.format("Failed to obtain OTS Edit Permission, tenant[%s]  username[%s]", userInfo.getTenantName(), userInfo.getUserName()));
                    throw new OtsException(RestErrorCode.EC_OTS_REST_GET_TABLE_PERMISSION_EXCEPTION, "Obtained cache tablePermission failed !");
                }
                info.setReadPermission(tablePermission.isOtsGetPermission());
                info.setWritePermission(tablePermission.isOtsPostPermission());
                info.setPermissionFlag(tablePermission.getPermissionFlag());
                if (tablePermission.getPermissionFlag()) {
                    if (!tablePermission.isOtsPostPermission()) {
                        LOG.error(String.format("Operation Failed Since No edit Permission, tenant[%s]  username[%s]", userInfo.getTenantName(), userInfo.getUserName()));
                        info.setErrorCode(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
                    }
                }
            }
        } catch (ConfigException | IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        }
        LOG.debug("Current uesr account owns table edit permission");
        info.setCurrentTime(useTime);
        return info;
    }

    public synchronized CacheInfo otsManageReadPermission(PermissionCheckUserInfo userInfo) throws OtsException {
        CacheInfo info = new CacheInfo();
        String useTime = sdf.format(new Date());
        CachePermission.CacheItem otsManagePermission;
        try {
            otsManagePermission = CachePermission.getInstance().getPermission(userInfo, RestConstants.OTS_MANAGE_PERMISSION_OPERATION, useTime);
            if (null == otsManagePermission) {
                LOG.error(String.format("Failed to obtain OTS Mange Read Permission, tenant[%s]  username[%s]", userInfo.getTenantName(), userInfo.getUserName()));
                throw new OtsException(RestErrorCode.EC_OTS_REST_GET_CACEH_PERMISSION_EXCEPTION, "Obtained cache otsPermission failed !");
            }
            info.setReadPermission(otsManagePermission.isOtsGetPermission());
            info.setWritePermission(otsManagePermission.isOtsPostPermission());
            info.setCurrentTime(useTime);
            if (!otsManagePermission.isOtsGetPermission()) {
                LOG.error(String.format("Operation Failed Since No OTS Mange Read Permission, tenant[%s]  username[%s]", userInfo.getTenantName(), userInfo.getUserName()));
                info.setErrorCode(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
            }
        } catch (ConfigException | IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        }
        LOG.debug("Current uesr account owns ots_manage_read permission");
        return info;
    }

    /**
     * 校验用户对表的增删改查权限
     * @param userInfo
     * @return CacheInfo
     * @throws OtsException
     */
    public synchronized CacheInfo otsManageWritePermission(PermissionCheckUserInfo userInfo) throws OtsException {
        CacheInfo info = new CacheInfo();
        String useTime = sdf.format(new Date());
        CachePermission.CacheItem otsManagePermission;
        try {
            otsManagePermission = CachePermission.getInstance().getPermission(userInfo, RestConstants.OTS_MANAGE_PERMISSION_OPERATION, useTime);
            if (null == otsManagePermission) {
                LOG.error(String.format("Failed to obtain OTS Mange Write Permission, tenant[%s] username[%s] ", userInfo.getTenantName(), userInfo.getUserName()));
                throw new OtsException(RestErrorCode.EC_OTS_REST_GET_CACEH_PERMISSION_EXCEPTION, "Obtained cache otsPermission failed !");
            }
            info.setReadPermission(otsManagePermission.isOtsGetPermission());
            info.setWritePermission(otsManagePermission.isOtsPostPermission());
            info.setCurrentTime(useTime);
            if (!otsManagePermission.isOtsPostPermission()) {
                LOG.error(String.format("Operation Failed Since No OTS Mange Write Permission, tenant[%s] username[%s] ", userInfo.getTenantName(), userInfo.getUserName()));
                info.setErrorCode(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
            }
        } catch (ConfigException | IOException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            info.setErrorCode(RestErrorCode.EC_OTS_AAS_REGISTER_RESSOURCE_FAILED);
        }
        LOG.debug("Current uesr account owns ots_manage_write permission");
        return info;
    }

    public static class CacheInfo {

        private long errorCode = CommonErrorCode.EC_COMMON_BASE_SUCCESS;
        private boolean readPermission = false;
        private boolean writePermission = false;
        private boolean permissionFlag = false;
        private String currentTime;

        public long getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(long errorCode) {
            this.errorCode = errorCode;
        }

        public boolean isReadPermission() {
            return readPermission;
        }

        public void setReadPermission(boolean readPermission) {
            this.readPermission = readPermission;
        }

        public boolean isWritePermission() {
            return writePermission;
        }

        public void setWritePermission(boolean writePermission) {
            this.writePermission = writePermission;
        }

        public String getCurrentTime() {
            return currentTime;
        }

        public void setCurrentTime(String currentTime) {
            this.currentTime = currentTime;
        }

        public boolean isPermissionFlag() {
            return permissionFlag;
        }

        public void setPermissionFlag(boolean permissionFlag) {
            this.permissionFlag = permissionFlag;
        }
    }

    public static PermissionCheckUserInfo getUserInfoModel(PermissionCheckUserInfo userInfo, HttpServletRequest request){
        long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
        long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
        String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
        String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
        userInfo.setUserId(userid);
        userInfo.setUserName(username);
        userInfo.setTenantId(tenantid);
        userInfo.setTenantName(tenant);
        userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
        return userInfo;
    }

    public synchronized CacheInfo otsPermissionHandler(PermissionCheckUserInfo userInfo,long tableId, PermissionOpesration opertation) throws OtsException{
        PermissionUtil.CacheInfo cacheInfo;
        switch (opertation){

            case READ:
                cacheInfo = checkReadPermission(userInfo, tableId);
                if (cacheInfo.getErrorCode() != CommonErrorCode.EC_COMMON_BASE_SUCCESS) {
                    throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT, 
                            "no permission to get table, data or index" );
                }
                return cacheInfo;
            case EDIT:
                cacheInfo = checkEditPermission(userInfo, tableId);
                if (cacheInfo.getErrorCode() != CommonErrorCode.EC_COMMON_BASE_SUCCESS) {
                    throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT, 
                            "no permission to edit table,data or index");
                }
                return cacheInfo;
            case OTSMANAGER:
                cacheInfo = otsManageReadPermission(userInfo);
                if (cacheInfo.getErrorCode() != CommonErrorCode.EC_COMMON_BASE_SUCCESS) {
                    throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT, 
                            "no table authorize permission");
                }
                return cacheInfo;
            case OTSMANAGEW:
                cacheInfo = otsManageWritePermission(userInfo);
                if (cacheInfo.getErrorCode() != CommonErrorCode.EC_COMMON_BASE_SUCCESS) {
                    throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT, 
                            "no table manage permission");
                }
                return cacheInfo;
            default:
                LOG.error(String.format("Undefined ots permission: %s",opertation.toString()));
                throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT, 
                           "permission operation is not defioned !!!(only permit choosing enum of Permission Operation)");
        }
    }
}
