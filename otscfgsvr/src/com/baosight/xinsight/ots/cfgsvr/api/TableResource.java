package com.baosight.xinsight.ots.cfgsvr.api;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.ConnException;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.common.RestErrorCode;
import com.baosight.xinsight.ots.cfgsvr.model.operate.TableBackupModel;
import com.baosight.xinsight.ots.cfgsvr.service.TableService;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.model.TableInfoListModel;
import com.baosight.xinsight.ots.rest.model.TableInfoModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.RegexUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author huangming
 */
// @Path 这里定义的是class级别的路径,体现在URI中指代资源路径.
@Path("/table")
public class TableResource extends com.baosight.xinsight.ots.rest.api.TableResource {
    private static final Logger LOG = Logger.getLogger(TableResource.class);

    // @Context用于注入上下文对象,例如ServletContext, Request, Response, UriInfo.
    @Context
    UriInfo uriInfo;
    @Context
    HttpServletRequest request;

    // @GET 表示该方法处理HTTP的GET请求.
    // @Path 这里是定义method级别的路径,体现在URI中指代资源子路径,可以带有{}模板匹配参数.
    // @Produces 指定返回的协议数据类型,可以定义为一个集合。
    // @PathParam 注入url中传递的参数.

    ///////////////表数据备份到服务器//////////////////////////////////////////
    @POST
    @Path("/_backup/{tablename}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response backup(@PathParam("tablename") String tablename, String body) {
        if (tablename.equals(RestConstants.Query_all_tables.toString())) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' can not contain illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "tablename '" + tablename + "' can not contain illegal char.");
            }
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            AasPermissionUtil.Permission permission = AasPermissionUtil.getPermissionByResource(
                    ConfigUtil.getInstance().getAuthServerAddr(), userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
            if (null == permission || !permission.isPost()) {
               throw new OtsException(OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT,"backup ots table failed");
            }

            String rediskeyTenantId = TableService.getRedisKeyTenantId(tenantid);
            if (!TableService.exist(userid, tenantid, tablename)) {
                LOG.error(Response.Status.CONFLICT.name() + ":" + tablename + " not exists.");
                return Response.status(Response.Status.CONFLICT).build();
            }
            String tenantState = ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE);
            if (tenantState != null && (tenantState.equals(RestConstants.DEFAULT_BACKUP_STATE_WAITING) || tenantState.equals(RestConstants.DEFAULT_BACKUP_STATE_RUNNING))) {
                LOG.error(Response.Status.CONFLICT.name() + ": A table is running backup/restore.");
                throw new OtsException(RestErrorCode.EC_OTS_TABLE_BACKUP_RUNNING, "A table is running backup/restore.");
            }
            LOG.debug("Post backup:" + tablename + "\nContent:\n" + body);
            TableBackupModel model = TableBackupModel.toClass(body);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.backupTable(userid, tenantid, tablename, model)).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (ConnException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(RestErrorCode.EC_OTS_TABLE_BACKUP_BACKUP_FAILED, e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }

    }

    ///////////////从服务器恢复表数据//////////////////////////////////////////
    @POST
    @Path("/_restore")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response restore(String body) {
        try {
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
            String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            String rediskeyTenantId = TableService.getRedisKeyTenantId(tenantid);
            String tenantState = ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE);
            if (tenantState != null && (tenantState.equals(RestConstants.DEFAULT_BACKUP_STATE_WAITING) || tenantState.equals(RestConstants.DEFAULT_BACKUP_STATE_RUNNING))) {
                LOG.error(Response.Status.CONFLICT.name() + ": A table is running backup/restore.");
                throw new OtsException(RestErrorCode.EC_OTS_TABLE_BACKUP_RUNNING, "A table is running backup/restore.");
            }
            LOG.debug("Post restore:\nContent:\n" + body);
            TableBackupModel model = TableBackupModel.toClass(body);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.restoreTable(userid, tenantid, tenant, username, model)).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (ConnException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(RestErrorCode.EC_OTS_TABLE_BACKUP_RESTORE_FAILED, e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    ///////////////获取表备份状态//////////////////////////////////////////
    @GET
    @Path("/status/{tablename}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTableBackupState(@PathParam("tablename") String tablename) {
        Map<String, Object> results = new HashMap<String, Object>();
        try {
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            String rediskeyTenantId = TableService.getRedisKeyTenantId(userInfo.getTenantId());
            if (tablename.equals(RestConstants.Query_all_tables.toString())) {
                TableInfoListModel tableList = com.baosight.xinsight.ots.rest.service.TableService.getAllTablesInfo(userInfo);
                List<Map<String, Object>> table_status_list = new ArrayList<Map<String, Object>>();
                if (tableList != null) {
                    List<TableInfoModel> tables = tableList.getTableinfolist();
                    if (tables.size() > 0) {
                        for (int i = 0; i < tables.size(); i++) {
                            Map<String, Object> record = new HashMap<String, Object>();
                            String rediskeyTablename = TableService.getRedisKeyTableName(userInfo.getTenantId(), tables.get(i).getId(), tables.get(i).getName());

                            record.put("table_name", tables.get(i).getName());
                            record.put("tenant_state", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE));
                            record.put("state", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE));
                            record.put("result", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT));
                            record.put("progress", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS));
                            table_status_list.add(record);
                        }
                    }
                }
                results.clear();
                results.put("errcode", 0);
                results.put("table_status_list", table_status_list);
            } else {
                if (!RegexUtil.isValidTableName(tablename)) {
                    LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' can not contain illegal char.");
                    throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "tablename '" + tablename + "' can not contain illegal char.");
                }
                if (!TableService.exist(userInfo.getUserId(), userInfo.getTenantId(), tablename)) {
                    LOG.error(Response.Status.CONFLICT.name() + ":" + tablename + " no exist.");
                    return Response.status(Response.Status.CONFLICT).build();
                }
                OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tablename);
                String rediskeyTablename = TableService.getRedisKeyTableName(userInfo.getTenantId(), table.getId(), table.getName());
                results.clear();
                results.put("errcode", 0);
                results.put("tenant_state", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE));
                results.put("state", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE));
                results.put("result", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT));
                results.put("progress", ConfigUtil.getInstance().getRedisUtil().getHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS));
            }
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(results).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(RestErrorCode.EC_OTS_TABLE_BACKUP_BACKUP_FAILED, e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }
}

