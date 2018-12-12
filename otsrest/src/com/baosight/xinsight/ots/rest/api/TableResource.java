package com.baosight.xinsight.ots.rest.api;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.model.TableConfigModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.TableUpdateModel;

import com.baosight.xinsight.ots.rest.body.table.TableCreateBodyModel;
import com.baosight.xinsight.ots.rest.service.TableService;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.RegexUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;


/**
 * @author huangming
 */
// @Path 这里定义的是class级别的路径,体现在URI中指代资源路径.
@Path("/table")
public class TableResource {
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
    @GET
    //here no need @Path()
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTableList() {
        try {
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            LOG.debug("Get table list, user:" + userInfo.getUserName() + "@" + userInfo.getTenantName());

            String name = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_name));
            String limit = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_limit));
            String offset = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_offset));

            if (limit != null && Long.parseLong(limit) > RestConstants.DEFAULT_QUERY_MAX_LIMIT) {
                LOG.info("limit is too large, set to " + RestConstants.DEFAULT_QUERY_MAX_LIMIT);
                limit = String.valueOf(RestConstants.DEFAULT_QUERY_MAX_LIMIT);
            }

            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(
                    TableService.getTablelist(userInfo, name,
                            limit == null ? RestConstants.DEFAULT_QUERY_LIMIT : Long.parseLong(limit),
                            offset == null ? RestConstants.DEFAULT_QUERY_OFFSET : Long.parseLong(offset))).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @GET
    @Path("/{tablename}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@PathParam("tablename") String tablename) {
        try {
            LOG.debug("Get:" + tablename);
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            if (tablename.equals(RestConstants.Query_all_tables)) {
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.getTablelist(userInfo)).build();
            } else {
                if (!RegexUtil.isValidTableName(tablename)) {
                    LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                    throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                }

                if (!TableService.exist(userInfo, tablename)) {
                    LOG.error(Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.");
                    return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
                            new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.")).build();
                } else {
                    return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.getTableInfo(userInfo, tablename)).build();
                }
            }
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    //2018.12.10 创建表 lyh
    // @Consumes 指定接受的数据类型,也可以定义为一个集合.
    @POST
    @Path("/{tablename}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("tablename") String tablename, String body) {
        //校验过表名后，肯定不会是tablename.equals(RestConstants.Query_all_tables)，这段冗余。
//        if (tablename.equals(RestConstants.Query_all_tables)) {
//            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
//            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON).entity(
//                    new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
//        }

        try {
            //校验表名合法性
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }

            //获得userInfo和body
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            LOG.debug("Post:" + tablename + "\nContent:\n" + body);

            //生成body对应的model并校验参数合法性
            TableCreateBodyModel bodyModel = TableCreateBodyModel.toClass(body);

            //创建表
            TableService.createTable(userInfo, tablename, bodyModel);
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }

        // successful
        return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(0L)).build();
    }


    @DELETE
    @Path("/{tablename}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("tablename") String tablename) {
        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON).entity(
                    new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            if (!TableService.exist(userInfo, tablename)) {
                LOG.error(Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.");
                return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
                        new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.")).build();
            }

            LOG.debug("Delete:" + tablename);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.deleteTable(userInfo, tablename)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    @PUT
    @Path("/{tablename}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response put(@PathParam("tablename") String tablename, String body) {
        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON).entity(
                    new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            if (!TableService.exist(userInfo, tablename)) {
                LOG.error(Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.");
                return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
                        new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.NOT_FOUND.name() + ":" + tablename + " is not exist.")).build();
            }

            LOG.debug("Put:" + tablename + "\nContent:\n" + body);
            TableUpdateModel model = TableUpdateModel.toClass(body);

            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(TableService.updateTable(userInfo, tablename, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    ///////////////获取所有表及其信息//////////////////////////////////////////
    @GET
    @Path("/_all_tables_info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllTablesInfo() {
        try {
            LOG.debug("Get: all tables information!");
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(TableService.getAllTablesInfo(userInfo)).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    ///////////////存取用户页面配置信息//////////////////////////////////////////
    @GET
    @Path("/{tablename}/display_columns")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfig(@PathParam("tablename") String tablename) {

        try {
            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());

            LOG.debug("Get:" + tablename + " config!");
            String columns = TableService.getConfig(userid, tenantid, tablename);
            if (null != columns) {
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(new TableConfigModel(columns, 0L)).build();
            } else {
                LOG.warn(Response.Status.NOT_FOUND.name() + ":" + tablename + " config is not exist.");
                return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
                        new ErrorMode((long) OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE_PROFILE, Response.Status.NOT_FOUND.name() + ":" + tablename + " config is not exist.")).build();
            }
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    @PUT
    @Path("/{tablename}/display_columns")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response putConfig(@PathParam("tablename") String tablename, String body) {

        try {
            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());

            LOG.debug("Put:" + tablename + " config!" + "\nContent:\n" + body);
            TableConfigModel model = TableConfigModel.toClass(body);

            TableService.saveConfig(userid, tenantid, tablename, model.getColumns());
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }

        return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(0L)).build();
    }
}

