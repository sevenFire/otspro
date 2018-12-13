package com.baosight.xinsight.ots.rest.api;

import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.model.RecordListModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.KeysQueryModel;
import com.baosight.xinsight.ots.rest.model.operate.RecordQueryModel;
import com.baosight.xinsight.ots.rest.service.RecordService;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.RegexUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.text.SimpleDateFormat;


/**
 * @author huangming
 */
// @Path 这里定义的是class级别的路径,体现在URI中指代资源路径.
@Path("/record")
public class RecordResource {
    private static final Logger LOG = Logger.getLogger(RecordResource.class);

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
    @Path("/{tablename}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@PathParam("tablename") String tablename) {
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

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            String hashkey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_hashkey));
            String rangekey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey));
            String rangekey_prefix = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_prefix));
            String range_key_start = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_start));
            String range_key_end = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_end));
            String columns = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_columns));
            String limit = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_limit));
            String offset = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_offset));
            String cursor_mark = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_cursormark));
            String descending = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_descending));
            String queryfrom = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_queryfrom));

            if (limit != null && Long.parseLong(limit) > RestConstants.DEFAULT_QUERY_MAX_LIMIT) {
                LOG.info("limit is too large, set to " + RestConstants.DEFAULT_QUERY_MAX_LIMIT);
                limit = String.valueOf(RestConstants.DEFAULT_QUERY_MAX_LIMIT);
            }

            RecordQueryModel model = new RecordQueryModel(hashkey, rangekey, rangekey_prefix, range_key_start, range_key_end, null, null,
                    columns,
                    limit == null ? null : Long.parseLong(limit),
                    offset == null ? null : Long.parseLong(offset),
                    cursor_mark,
                    descending == null ? false : true,
                    descending == null ? null : Boolean.parseBoolean(descending),
                    queryfrom == null ? null : Integer.parseInt(queryfrom));

            LOG.debug("Get:" + tablename + "\nContent:\n" + model.toString());

            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(RecordService.getRecords(userInfo, tablename, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @GET
    @Path("/file/{tablename}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response getFile(@PathParam("tablename") String tablename) {
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

            String hashkey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_hashkey));
            String rangekey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey));
            String column = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_column));

            byte[] fileContent = RecordService.getRecordFile(userInfo, tablename, hashkey, rangekey, column);
            LOG.debug("getRecordFile size: " + fileContent.length);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_OCTET_STREAM).entity(fileContent).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @POST
    @Path("/file/{tablename}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response postFile(@PathParam("tablename") String tablename, byte[] data) {
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

            LOG.debug("put record cell, data size: " + data.length);

            String hashkey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_hashkey));
            String rangekey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey));
            String column = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_column));

            // successful
            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(RecordService.putRecordFile(userInfo, tablename, hashkey, rangekey, column, data)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @POST
    @Path("/{tablename}/keys")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response queryKeys(@PathParam("tablename") String tablename, String body) {
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
            String columns = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_columns));
            String descending = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_descending));
            String queryfrom = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_queryfrom));


            if (body.length() > 1000) {
                LOG.debug("Post:" + tablename + "\n Part Content:\n" + body.substring(0, 999));
            } else {
                LOG.debug("Post:" + tablename + "\nContent:\n" + body);
            }
            KeysQueryModel model = KeysQueryModel.toClass(body);
            RecordQueryModel query = new RecordQueryModel();
            query.set_full_keys(model.getKeylist());
            query.setColumns(columns);
            query.setDescending(descending == null ? null : Boolean.parseBoolean(descending));
            query.setIsSort(descending == null ? false : true);
            query.setQuery_from(queryfrom == null ? null : Integer.parseInt(queryfrom));

            // successful

            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(RecordService.getRecords(userInfo, tablename, query)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    // @Consumes 指定接受的数据类型,也可以定义为一个集合.
    @POST
    @Path("/{tablename}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("tablename") String tablename, String body) {
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

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);

            if (body.length() > 1000) {
                LOG.debug("Post:" + tablename + "\n Part Content:\n" + body.substring(0, 999));
            } else {
                LOG.debug("Post:" + tablename + "\nContent:\n" + body);
            }
            RecordListModel model = RecordListModel.toClass(body);

            // successful
            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(RecordService.insertRecords(userInfo, tablename, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
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

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            String hashkey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_hashkey));
            String rangekey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey));
            String rangekey_prefix = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_prefix));
            String range_key_start = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_start));
            String range_key_end = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_end));
            String start_time = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_start_time));
            String end_time = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_end_time));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:S");
            Long lStarttime = (start_time != null ? sdf.parse(start_time).getTime() : null);
            if (lStarttime != null && lStarttime > RestConstants.DEFAULT_MAX_MILLISECOND) {
                LOG.info("start is too large, set to " + RestConstants.DEFAULT_MAX_MILLISECOND);
                lStarttime = RestConstants.DEFAULT_QUERY_MAX_LIMIT;
            }
            Long lEndtime = (end_time != null ? sdf.parse(end_time).getTime() : null);
            if (lEndtime != null && lEndtime > RestConstants.DEFAULT_MAX_MILLISECOND) {
                LOG.info("end is too large, set to " + RestConstants.DEFAULT_MAX_MILLISECOND);
                lEndtime = RestConstants.DEFAULT_QUERY_MAX_LIMIT;
            }

            RecordQueryModel model = new RecordQueryModel(hashkey, rangekey, rangekey_prefix, range_key_start, range_key_end, lStarttime, lEndtime);

            LOG.debug("Delete:" + tablename + "\nContent:\n" + model.toString());
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(RecordService.deleteRecords(userInfo, tablename, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @DELETE
    @Path("/{tablename}/truncate")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAllRecords(@PathParam("tablename") String tablename) {
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

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            LOG.debug("Delete: truncate " + tablename + "\n");
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(RecordService.deleteAllRecords(userInfo, tablename)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }


    @DELETE
    @Path("/{tablename}/keys")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteKeys(@PathParam("tablename") String tablename, String body) {
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

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            if (body.length() > 1000) {
                LOG.debug("Delete:" + tablename + "\n Part Content:\n" + body.substring(0, 999));
            } else {
                LOG.debug("Delete:" + tablename + "\nContent:\n" + body);
            }
            KeysQueryModel model = KeysQueryModel.toClass(body);
            RecordQueryModel query = new RecordQueryModel();
            query.set_full_keys(model.getKeylist());

            // successful
            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(RecordService.deleteRecords(userInfo, tablename, query)).build();

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
            return Response.status(Response.Status.FORBIDDEN).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "tablename '" + tablename + "' contains illegal char.");
            }

            //String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            //String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);

            if (body.length() > 1000) {
                LOG.debug("Put:" + tablename + "\n Part Content:\n" + body.substring(0, 999));
            } else {
                LOG.debug("Put:" + tablename + "\nContent:\n" + body);
            }
            RecordListModel model = RecordListModel.toClass(body);
            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(RecordService.updateRecords(userInfo, tablename, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }
}

