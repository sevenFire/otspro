package com.baosight.xinsight.ots.rest.permission;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import com.baosight.xinsight.utils.EhcacheUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.ehcache.Cache.Entry;
import org.ehcache.expiry.Duration;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * OtsRestCachePermission
 *
 * @author zhangyunlong
 * @created 2017.04.28
 */

public class CachePermission {
    private static final Logger LOG = Logger.getLogger(CachePermission.class);

    private volatile static CachePermission instance;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private EhcacheUtil ehcacheManager;

    private CachePermission() throws NumberFormatException, IOException {
        ehcacheManager = new EhcacheUtil();
        String maxMemoryElementSize = ConfigUtil.getInstance().getValue(OtsConstants.OTS_EHC_MAX_MEMO_ELEMENT_NUMB, "30000").trim();
        String ehcacheMaxAvailableTime = ConfigUtil.getInstance().getValue(OtsConstants.OTS_EHC_MAX_INTERVAL_TIME, "300").trim();
        ehcacheManager.createCache(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME, String.class, CacheItem.class,
                Long.parseLong(maxMemoryElementSize),
                EhcacheUtil.makeExpirationOfIdle(Duration.of(Long.parseLong(ehcacheMaxAvailableTime), TimeUnit.SECONDS)));
        LOG.info((new StringBuilder()).append("create a single ehcache manager !!!").toString());
        LOG.info("begin to read the ots config >>>>>>");
        LOG.info("********************************");
        LOG.info(String.format("create a single ehcache manager for OTS; the maximum number of mappings to cache [%s])(number)  max available time [%s](s)", maxMemoryElementSize, ehcacheMaxAvailableTime));
        LOG.info("********************************");
        LOG.info("<<<<<< end to read the ots config");
    }

    public static CachePermission getInstance() throws NumberFormatException, IOException {
        if (null == instance) {
            synchronized (CachePermission.class) {
                if (null == instance) {
                    instance = new CachePermission();
                    LOG.debug((new StringBuilder()).append("create a single Cache, HashCode: ").append(String.valueOf(instance.hashCode())).toString());
                }
            }
        }
        return instance;
    }

    public EhcacheUtil getEhcacheManager() {
        return this.ehcacheManager;
    }

    // mainly using for kafka aynchronized permission ot other nodes cache when any node ots permission changed
    public CacheItem getPermission(PermissionCheckUserInfo userInfo, long tableId, boolean readPermission, boolean writePermission, boolean permissionFlag, String useTime) throws ConfigException, IOException, Exception {
        CacheItem result = null;
        if (RestConstants.OTS_MANAGE_PERMISSION_OPERATION == tableId) {
            result = this.ehcacheManager.getElement(
                    OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                    (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
                            .append(RestConstants.OTS_MANAGE_PERMISSION_OPERATION).toString(), String.class, CacheItem.class);
        } else {
            result = this.ehcacheManager.getElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                    (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
                            .append(tableId).toString(), String.class,
                    CacheItem.class);
        }

        if (null != result && readPermission == result.isOtsGetPermission() && writePermission == result.isOtsPostPermission()) {
            LOG.debug((new StringBuilder()).append("the permission ").append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(String.valueOf(userInfo.getUserId())).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
                    .append(tableId).append(" RP= ").append(result.isOtsGetPermission()).append(" WP= ").append(result.isOtsPostPermission()).append(" PF=").append(result.getPermissionFlag())
                    .append(" exist in perission cache and use it directly!").toString());
        } else {
            result = new CacheItem();
            try {
                result.setCreatTime(useTime);
                result.setTenant(userInfo.getTenantName());
                result.setUsername(userInfo.getUserName());
                result.setOtsGetPermission(readPermission);
                result.setOtsPostPermission(writePermission);
                result.setPermissionFlag(permissionFlag);
                result.setUserId(userInfo.getUserId());
                result.setTableId(tableId);
                result.setServiceName(userInfo.getServiceName());
                this.ehcacheManager.putElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                        (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).toString(), result,
                        String.class, CacheItem.class);
                LOG.debug((new StringBuilder()).append("search new permission ").append(String.valueOf(userInfo.getUserId())).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).append(" RP=")
                        .append(readPermission).append(" WP=").append(writePermission).append(" PF=").append(permissionFlag).append(" has been saved into cache").toString());
            } catch (Exception e) {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
                return null;
            }
        }
        return result;
    }

    // mainly using for local service get permission info
    public CacheItem getPermission(PermissionCheckUserInfo userInfo, long tableId, String useTime) throws ConfigException, IOException, Exception {
        CacheItem result = null;
        if (RestConstants.OTS_MANAGE_PERMISSION_OPERATION == tableId) {
            result = this.ehcacheManager.getElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                    (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
                            .append(RestConstants.OTS_MANAGE_PERMISSION_OPERATION).toString(), String.class, CacheItem.class);
        } else {
            result = this.ehcacheManager.getElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                    (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
                            .append(tableId).toString(), String.class, CacheItem.class);
        }
        // if result equals to null permission will be obtain from AAS, otherwise, it will get from cache
        if (null == result) {
            Date creatTime = new Date();
            try {
                if (RestConstants.OTS_MANAGE_PERMISSION_OPERATION == tableId) {
                    result = otsManagePermission(userInfo.getUserId(), userInfo.getTenantId(), userInfo.getTenantName(), userInfo.getUserName(), tableId, sdf.format(creatTime));
                    if (null != result) {
                        LOG.debug((new StringBuilder()).append("Permission key as:").append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(String.valueOf(userInfo.getUserId()))
                                .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).append(" RP=").append(result.isOtsGetPermission()).append(" WP=")
                                .append(result.isOtsPostPermission()).append(" PF=").append(result.getPermissionFlag()).append(" check ots_manage permission!").toString());
                    }
                } else {
                    result = tablePermissionCheck(userInfo.getUserId(), userInfo.getTenantId(), userInfo.getTenantName(), userInfo.getUserName(), tableId, sdf.format(creatTime));
                    if (null != result) {
                        LOG.debug((new StringBuilder()).append("Permission key as:").append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(String.valueOf(userInfo.getUserId()))
                                .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).append(" RP=").append(result.isOtsGetPermission()).append(" WP=")
                                .append(result.isOtsPostPermission()).append(" PF=").append(result.getPermissionFlag()).append(" check table permission!").toString());
                    }
                }
            } catch (Exception e) {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
                return null;
            }
        } else {
            LOG.debug((new StringBuilder()).append("permission exists in the cache and use it directly! ").append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(String.valueOf(userInfo.getUserId()))
                    .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).toString());
        }
        return result;
    }

    /**
     * @param userId
     * @param tenantId
     * @param tenant
     * @param username
     * @param tableId
     * @param createTime
     * @return
     * @throws Exception
     */
    private CacheItem tablePermissionCheck(long userId, long tenantId, String tenant, String username, long tableId, String createTime) throws Exception {
        PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
        userInfo.setUserId(userId);
        userInfo.setTenantId(tenantId);
        userInfo.setTenantName(tenant);
        userInfo.setUserName(username);
        userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
        AasPermissionUtil.Permission permission = AasPermissionUtil.getPermissionByResource(
                ConfigUtil.getInstance().getAuthServerAddr(), userInfo,
                AasPermissionUtil.OTS_INSTANCE_RESOURCE_PREFIX + tableId);
        Boolean permissionFlag = ConfigUtil.getInstance().getOtsAdmin().checkTablePermitted(tableId);
        // update permission into ehcache
        if (null == permission) {
            String resourceKey = (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId())
                    .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).toString();
            this.remove(resourceKey);
            LOG.warn(String.format("obtain permission failed, the relevat permission of [%s] will be removed if existed in cache", resourceKey));
            return null;
        }
        CacheItem properties = putCacheItem(userInfo.getUserId(), userInfo.getTenantId(), userInfo.getTenantName(), 
                userInfo.getUserName(), tableId, permission.isGet(), permission.isPost(), permissionFlag, createTime);
        LOG.debug((new StringBuilder()).append(String.valueOf(userInfo.getUserId()))
                .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).append(":")
                .append(properties.toString()).toString());
        return properties;
    }

    /**
     * @param userId
     * @param tenantId
     * @param tenant
     * @param username
     * @param tableId
     * @param createTime
     * @return
     * @throws Exception
     */
    public CacheItem otsManagePermission(long userId, long tenantId, String tenant, String username, long tableId, String createTime) throws Exception {
        CacheItem properties = new CacheItem();
        PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
        userInfo.setUserId(userId);
        userInfo.setTenantId(tenantId);
        userInfo.setTenantName(tenant);
        userInfo.setUserName(username);
        userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
        AasPermissionUtil.Permission permission = AasPermissionUtil.getPermissionByResource(ConfigUtil.getInstance()
                .getAuthServerAddr(), userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
        // update permission into ehcache
        if (null == permission) {
            String resourceKey = (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userInfo.getUserId())
                    .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).toString();
            this.remove(resourceKey);
            LOG.warn(String.format("obtain permission failed, the relevat permission of [%s] will be removed if existed in cache", resourceKey));
            return null;
        }
        properties = putCacheItem(userInfo.getUserId(), userInfo.getTenantId(), userInfo.getTenantName(), 
                userInfo.getUserName(), tableId, permission.isGet(), permission.isPost(), false, createTime);
        LOG.debug((new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(String.valueOf(userInfo.getUserId()))
                .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).append(":")
                .append(properties.toString()).toString());
        return properties;
    }

    private CacheItem putCacheItem(long userId, long tenantId, String teanant, String username, long tableId, boolean otsmanageGet, boolean otsmanagePost, boolean permissionFlag, String creatTime) {
        CacheItem properties = new CacheItem();
        properties.setTenant(teanant);
        properties.setUsername(username);
        properties.setCreatTime(creatTime);
        properties.setUserId(userId);
        properties.setTenantId(tenantId);
        properties.setPermissionFlag(permissionFlag);
        properties.setOtsGetPermission(otsmanageGet);
        properties.setOtsPostPermission(otsmanagePost);
        properties.setTableId(tableId);
        this.ehcacheManager.putElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME,
                (new StringBuilder()).append(RestConstants.OTS_PERMISSION_CACHE_KEY).append(userId)
                        .append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT).append(tableId).toString(),
                properties, String.class, CacheItem.class);
        return properties;
    }

    public boolean remove(String key) {
        try {
            this.ehcacheManager.removeElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME, key, String.class, CacheItem.class);
            return true;
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            return false;
        }
    }

    public void batchRemove(String key) {
        Iterator<Entry<String, CacheItem>> iter = null;
        try {
            iter = this.ehcacheManager.iterator(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME, String.class, CacheItem.class);
            if (iter != null) {
                while (iter.hasNext()) {
                    Entry<String, CacheItem> element = iter.next();
                    String elementKey = element.getKey();
                    // 表权限修改后，将缓存内所有涉及此表的权限删除，当用到的时候从数据库从新查找并存入缓存，这样保证权限一致性并且当表的数量较大时减小查询消耗
                    if (elementKey.substring(elementKey.indexOf(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT) + 1, elementKey.length()).equals(key)) {
                        CachePermission.getInstance().remove(elementKey);
                        LOG.trace((new StringBuilder()).append(elementKey).append(" has been removed from permission cache successfully !"));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public void updateCachePermission(Iterator<Entry<String, CacheItem>> iter, String calledServiceName) {
        LinkedList<UpdateItem> updateList = null;
        // generate an update list
        if (iter != null) {
            updateList = new LinkedList<UpdateItem>();
            Date currentTimeDate = new Date();
            StringBuffer otscfgsvrEhcachePermissionInfo = new StringBuffer("");
            while (iter.hasNext()) {
                Entry<String, CacheItem> element = iter.next();
                String elementKey = element.getKey();
                if (element.getKey() != null && element.getValue() != null) {
                    if (-1 == element.getValue().getTableId()) {
                        otscfgsvrEhcachePermissionInfo.append(String.format("key:[%s] CacheItem:[%s]", element.getKey(), element.getValue().toString()));
                    }
                }
                // keep non expiry permission adding into update list
                CacheItem result = this.ehcacheManager.getElement(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME, elementKey, String.class, CacheItem.class);
                if (null == result) {
                    LOG.debug((new StringBuilder()).append(" ").append(calledServiceName).append(" cache permission ").append(elementKey).append(" is expiry and removed !!!").toString());
                } else {
                    try {
                        if ((currentTimeDate.getTime() - sdf.parse(result.getCreatTime()).getTime()) >=
                                Long.valueOf(ConfigUtil.getInstance().getValue(ConfigConstants.OTS_PERMISSION_UPDATE_INTERVAL, "600000"))) {
                            CachePermission.UpdateItem item = new CachePermission.UpdateItem();
                            item.setItemKey(elementKey);
                            item.setTenant(result.tenant);
                            item.setUsername(result.username);
                            item.setUserId(result.getUserId());
                            item.setTenantId(result.getTenantId());
                            item.setTableId(result.getTableId());
                            // item.set setUpdateTime(result.getCreatTime());
                            updateList.add(item);
                            LOG.debug((new StringBuilder()).append(" ").append(calledServiceName).append(" ").append(elementKey).append(" add into permission update list, ").append(" create time as:").append(result.getCreatTime()).toString());
                        }
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        LOG.debug(ExceptionUtils.getFullStackTrace(e));
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOG.debug(ExceptionUtils.getFullStackTrace(e));
                    } catch (ParseException e) {
                        e.printStackTrace();
                        LOG.debug(ExceptionUtils.getFullStackTrace(e));
                    }
                }
            }
            LOG.debug(String.format("[%s] ehcache permission info:[%s]", calledServiceName ,otscfgsvrEhcachePermissionInfo.toString()));
        }
        // update cache permission with update list
        if (null != updateList && !updateList.isEmpty()) {
            StringBuilder listString = new StringBuilder(String.format("[%s] permission finished update list:[", calledServiceName));
            CacheItem returnPermissionResult = null;
            for (UpdateItem item : updateList) {
                try {
                    if (RestConstants.OTS_MANAGE_PERMISSION_OPERATION == item.getTableId()) {
                        returnPermissionResult = otsManagePermission(item.getUserId(), item.getTenantId(), item.getTenant(), item.getUsername(), item.getTableId(), sdf.format(new Date()));
                    } else {
                        returnPermissionResult = tablePermissionCheck(item.getUserId(), item.getTenantId(), item.getTenant(), item.getUsername(), item.getTableId(), sdf.format(new Date()));
                    }
                    if (null != returnPermissionResult) {
                        Date executeTime = new Date();
                        listString.append(item.getItemKey()).append(" permission cache has been updated ")
                                .append(sdf.format(executeTime)).append("\r\n");
                    }
                } catch (NumberFormatException e) {
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                } catch (Exception e) {
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                }
            }
            LOG.debug(listString.append("]").toString());
        }
    }

    public static class CacheItem implements Serializable {
        /**
         * permission cache internal class for saving table permission info
         */
        private static final long serialVersionUID = 1L;
        private long userId;
        private long tenantId;
        private long tableId;
        private boolean otsGetPermission;
        private boolean otsPostPermission;
        private Boolean permissionFlag = false;
        private String creatTime;
        private String tenant;
        private String username;
        private String serviceName;

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getTenant() {
            return tenant;
        }

        public void setTenant(String tenant) {
            this.tenant = tenant;
        }

        public Boolean getPermissionFlag() {
            return permissionFlag;
        }

        public void setPermissionFlag(Boolean permissionFlag) {
            this.permissionFlag = permissionFlag;
        }

        public String getCreatTime() {
            return creatTime;
        }

        public void setCreatTime(String creatTime) {
            this.creatTime = creatTime;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public long getTenantId() {
            return tenantId;
        }

        public void setTenantId(long tenantId) {
            this.tenantId = tenantId;
        }

        public long getTableId() {
            return tableId;
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        public boolean isOtsPostPermission() {
            return otsPostPermission;
        }

        public void setOtsPostPermission(boolean otsPostPermission) {
            this.otsPostPermission = otsPostPermission;
        }

        public boolean isOtsGetPermission() {
            return otsGetPermission;
        }

        public void setOtsGetPermission(boolean otsGetPermission) {
            this.otsGetPermission = otsGetPermission;
        }

        @Override
        public String toString() {
            return "CacheItem{" +
                    "userId=" + userId +
                    ", tenantId=" + tenantId +
                    ", tableId=" + tableId +
                    ", otsGetPermission=" + otsGetPermission +
                    ", otsPostPermission=" + otsPostPermission +
                    ", permissionFlag=" + permissionFlag +
                    ", creatTime='" + creatTime + '\'' +
                    ", tenant='" + tenant + '\'' +
                    ", username='" + username + '\'' +
                    ", serviceName='" + serviceName + '\'' +
                    '}';
        }
    }

    public static class UpdateItem implements Serializable {
        /**
         * permission cache internal class for saving update table permission
         * info
         */
        private static final long serialVersionUID = 2L;
        private String itemKey;
        private String tenant;
        private String username;
        private String serviceName;
        private long userId;
        private long tenantId;
        private long tableId;

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getItemKey() {
            return itemKey;
        }

        public void setItemKey(String itemKey) {
            this.itemKey = itemKey;
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

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public long getTenantId() {
            return tenantId;
        }

        public void setTenantId(long tenantId) {
            this.tenantId = tenantId;
        }

        public long getTableId() {
            return tableId;
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        @Override
        public String toString() {
            return "UpdateItem{" +
                    "itemKey='" + itemKey + '\'' +
                    ", tenant='" + tenant + '\'' +
                    ", username='" + username + '\'' +
                    ", serviceName='" + serviceName + '\'' +
                    ", userId=" + userId +
                    ", tenantId=" + tenantId +
                    ", tableId=" + tableId +
                    '}';
        }
    }
}
