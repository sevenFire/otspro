package com.baosight.xinsight.ots.rest.api;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.OtsIndex;
import com.baosight.xinsight.ots.client.OTSTable;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.model.IndexConfigModel;
import com.baosight.xinsight.ots.rest.model.IndexInfoModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.IndexQueryModel;
import com.baosight.xinsight.ots.rest.model.operate.IndexUpdateModel;
import com.baosight.xinsight.ots.rest.model.operate.SecondIndexQueryModel;
import com.baosight.xinsight.ots.rest.service.IndexService;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.RegexUtil;
import com.baosight.xinsight.yarn.YarnAppUtil;
import com.baosight.xinsight.yarn.YarnTagGenerator;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.Map;

/**
 * @author huangming
 */

@Path("/index")
public class IndexResource {
    private static final Logger LOG = Logger.getLogger(IndexResource.class);

    @Context
    UriInfo uriInfo;
    @Context
    HttpServletRequest request;

    @GET
    @Path("/{tablename}/{indexname}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname) {
        if (tablename.equals(RestConstants.Query_all_tables.toString())) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }
            LOG.debug("Get:" + tablename + "/" + indexname);
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            // if list all indexes of table
            if (indexname.equals(RestConstants.Query_all_indexes.toString())) {
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(IndexService.getIndexlist(userInfo, tablename)).build();
            } else {
                if (!RegexUtil.isValidIndexName(indexname)) {
                    LOG.error(Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                    throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                }

                String query = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_query));
                // check if action was query based on index
                if (null != query) {
                    String columns = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_columns));
                    String limit = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_limit));
                    String offset = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_offset));
                    String cursor_mark = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_indexcursor));

                    if (limit != null && Long.parseLong(limit) > RestConstants.DEFAULT_QUERY_MAX_LIMIT) {
                        LOG.info("limit is too large, set to " + RestConstants.DEFAULT_QUERY_MAX_LIMIT);
                        limit = String.valueOf(RestConstants.DEFAULT_QUERY_MAX_LIMIT);
                    }

                    OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tablename);
                    if (null == table) {
                        LOG.error("Table " + tablename + "no exist!");
                        throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Table " + tablename + " no exist!");
                    }
                    OtsIndex otsIndex = table.getIndex(indexname);
                    if (null == otsIndex) {
                        LOG.error("Index " + tablename + "." + indexname + " no exist!");
                        throw new OtsException(OtsErrorCode.EC_OTS_INDEX_NO_EXIST, "Index " + tablename + "." + indexname + " no exist!");
                    }

                    if ((char) OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX == otsIndex.getPattern()) {
                        String hashkey = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_hashkey));
                        String rangekey_start = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_start));
                        String rangekey_end = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_rangekey_end));
                        if (query.equals("*:*")) {
                            query = null;
                        }
                        SecondIndexQueryModel secondQuery = new SecondIndexQueryModel(hashkey, rangekey_start, rangekey_end, columns, query, limit == null ? null : Integer.parseInt(limit),
                                offset == null ? null : Integer.parseInt(offset), cursor_mark);
                        LOG.debug("Content:\n" + secondQuery.toString());
                        return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON)
                                .entity(IndexService.getSecondaryIndexQuery(userInfo, tablename, indexname, secondQuery)).build();
                    } else {
                        String filters = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_filters));
                        String orders = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_orders));
                        IndexQueryModel model = new IndexQueryModel(query, filters, columns, orders, limit == null ? null : Long.parseLong(limit), offset == null ? null : Long.parseLong(offset), cursor_mark);
                        LOG.debug("Content:\n" + model.toString());

                        return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(IndexService.getSolrIndexQuery(userInfo, tablename, indexname, model)).build();
                    }

                } else {
                    String queryfrom = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_queryfrom));
                    return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(IndexService.getIndexInfo(userInfo, tablename, indexname, queryfrom)).build();
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

    @POST
    @Path("/{tablename}/{indexname}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname, String body) {
        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }

            if (indexname.equals(RestConstants.Query_all_indexes)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.");
                return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.")).build();
            }

            if (!RegexUtil.isValidIndexName(indexname)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
            }

            LOG.debug("Post:" + tablename + "/" + indexname + "\nContent:\n" + body);
            IndexInfoModel model = IndexInfoModel.toClass(body);
            model.setName(indexname);
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(IndexService.createIndex(userInfo, tablename, indexname, model)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @DELETE
    @Path("/{tablename}/{indexname}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname) {
        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }

            // add your code here
            if (indexname.equals(RestConstants.Query_all_indexes)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.");
                return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.")).build();
            }

            if (!RegexUtil.isValidIndexName(indexname)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
            }

            LOG.debug("Delete:" + tablename + "/" + indexname);
            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(IndexService.deleteIndex(userInfo, tablename, indexname)).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @PUT
    @Path("/{tablename}/{indexname}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response put(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname, String body) {
        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }

            if (indexname.equals(RestConstants.Query_all_indexes)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.");
                return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ":" + indexname + " is not a valid index object.")).build();
            } else {
                if (!RegexUtil.isValidIndexName(indexname)) {
                    LOG.error(Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                    throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.FORBIDDEN.name() + ": indexname '" + indexname + "' contain illegal char.");
                }

                LOG.debug("Put:" + tablename + "/" + indexname + "\nContent:\n" + body);
                IndexUpdateModel model = IndexUpdateModel.toClass(body);

                PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
                userInfo = PermissionUtil.getUserInfoModel(userInfo, request);

                return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(IndexService.updateIndex(userInfo, tablename, indexname, model)).build();
            }
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @GET
    @Path("/{tablename}/_all_indexes_info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllIndexesInfo(@PathParam("tablename") String tablename) {

        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        // add your code here
        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }

            PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
            userInfo = PermissionUtil.getUserInfoModel(userInfo, request);

            LOG.debug("Get:" + tablename + " all indexes information!");
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(IndexService.getAllIndexesInfo(userInfo, tablename)).build();

        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }

    @GET
    @Path("/status/{tablename}/{indexname}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getIndexBuildStatus(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname) {

        if (tablename.equals(RestConstants.Query_all_tables)) {
            LOG.error(Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.");
            return Response.status(Response.Status.FORBIDDEN).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorMode(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ":" + tablename + " is not a valid table object.")).build();
        }

        String indexTypeQuery = StringUtils.trim(uriInfo.getQueryParameters().getFirst(RestConstants.Query_index_type));

        int indexType = RestConstants.OTS_INDEX_TYPE_SOLR;
        if (!indexTypeQuery.isEmpty()) {
            indexType = Integer.parseInt(indexTypeQuery);
        }
        HttpClient client = new DefaultHttpClient();
        HttpGet getStatusMethod = null;
        HttpGet getYarnMethod = null;
        HttpResponse response = null;

        Map<String, Object> appInfo = new HashMap<String, Object>();
        Map<String, String> results = new HashMap<String, String>();

        try {
            if (!RegexUtil.isValidTableName(tablename)) {
                LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
            }
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
            LOG.debug("Get index build status:" + tablename + "/" + indexname);

            YarnAppUtil yarnAppUtil = new YarnAppUtil(ConfigUtil.getInstance().getYarnWebappAddr(), ConfigUtil.getInstance().getRedisHost());

            if (indexType == RestConstants.OTS_INDEX_TYPE_SOLR) {
                String indexerServerAddr = ConfigUtil.getInstance().getIndexerServerAddrRandom();
                String indexName = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOUBLE_UNLINE_SPLIT + tablename + CommonConstants.DEFAULT_DOUBLE_UNLINE_SPLIT + indexname;
                getStatusMethod = new HttpGet((new StringBuilder()).append("http://").append(indexerServerAddr).append("/indexer/").append(indexName).toString());
                response = client.execute(getStatusMethod);
                String indexerInfo = EntityUtils.toString(response.getEntity(), "utf-8");
                EntityUtils.consume(response.getEntity());
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(indexerInfo);
                String lifecycleState = rootNode.path("lifecycleState").getValueAsText();
                if (lifecycleState.equals("ACTIVE")) {
                    String indexingState = rootNode.path("batchIndexingState").getValueAsText();
                    if (indexingState.equals("INACTIVE")) {
                        JsonNode lastBuildNode = rootNode.path("lastBatchBuild");
                        boolean success = false;
                        if (lastBuildNode != null) {
                            success = lastBuildNode.path("finishedSuccessful").getValueAsBoolean();
                        }
                        if (success) {
                            results.clear();
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_COMPLETE);
                        } else {
                            results.clear();
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_FAILED);
                        }
                    } else if (indexingState.equals("BUILDING")) {
                        JsonNode activeBuildNode = rootNode.path("activeBatchBuild");
                        JsonNode childrenNode = mapper.readTree(activeBuildNode.toString()).path("mapReduceJobTrackingUrls");
                        String mapReduceJobTrackingUrls = childrenNode.toString();
                        int colonPos = mapReduceJobTrackingUrls.indexOf(":");
                        results.clear();
                        if (colonPos > 0) {
                            String jobId = mapReduceJobTrackingUrls.substring(2, colonPos - 1);
                            String url = mapper.readTree(mapReduceJobTrackingUrls).path(jobId).getValueAsText();
                            int portPos = url.indexOf(":8088");
                            String yarnAddr = url.substring(0, portPos + 5);
                            String appId = "application_" + jobId.substring(4);

                            getYarnMethod = new HttpGet((new StringBuilder()).append(yarnAddr).append("/ws/v1/cluster/apps/").append(appId).toString());
                            response = client.execute(getYarnMethod);
                            String yarnInfo = EntityUtils.toString(response.getEntity(), "utf-8");
                            EntityUtils.consume(response.getEntity());
                            JsonNode rootYarnInfo = mapper.readTree(yarnInfo).path("app");

                            String indexState = rootYarnInfo.path("progress").getValueAsText();

                            results.clear();
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, indexState);
                        } else {
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_INIT);
                        }
                    } else {
                        results.clear();
                        results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_INIT);
                    }
                } else {
                    results.clear();
                    results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_INIT);
                }
            } else if (indexType == RestConstants.OTS_INDEX_TYPE_HBASE) {
                long table_id = Long.parseLong(uriInfo.getQueryParameters().getFirst(RestConstants.Query_table_id));
                long index_id = Long.parseLong(uriInfo.getQueryParameters().getFirst(RestConstants.Query_index_id));

                String tablenameFull = (new StringBuilder()).append(String.valueOf(tenantid)).append(":").append(tablename).toString();
                //String appTag = (new StringBuilder()).append("BuildIndex_").append(tablenameFull).append(":").append(indexname).toString();
                String appTag = YarnTagGenerator.GenSecIndexBuildMapreduceTag(table_id, tablenameFull, index_id, indexname);
                appInfo = yarnAppUtil.getAppStateByAppTag(appTag);
                if (!appInfo.isEmpty()) {
                    results.clear();
                    String appState = appInfo.get(YarnAppUtil.XINSIGHT_YARN_APP_INFO_STATE).toString();
                    if (appState.equals(YarnAppUtil.YARN_APP_STATE_INITIALIZING)) {
                        results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_INIT);
                    } else if (appState.equals(YarnAppUtil.YARN_APP_STATE_UNKNOWN) || appState.equals(YarnAppUtil.YARN_APP_STATE_FAILED)) {
                        results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_FAILED);
                    } else {
                        String finalStatus = appInfo.get(YarnAppUtil.XINSIGHT_YARN_APP_INFO_FINAL_STATUS).toString();

                        if (appState.equals(YarnAppUtil.YARN_APP_STATE_FINISHED)) {
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_COMPLETE);
                            if (finalStatus.equals(YarnAppUtil.YARN_APP_STATE_FAILED) || finalStatus.equals(YarnAppUtil.YARN_APP_STATE_KILLED)) {
                                results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, RestConstants.DEFAULT_PROGRESS_FAILED);
                            }
                        } else {
                            results.put(RestConstants.DEFAULT_KEYWORD_PROGRESS, appInfo.get(YarnAppUtil.XINSIGHT_YARN_APP_INFO_PROGRESS).toString());
                        }
                    }
                } else {
                    LOG.error(Response.Status.NOT_FOUND.name() + ": yarn appTag: '" + appTag + "' not found.");
                    throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.NOT_FOUND.name() + ": yarn appTag: '" + appTag + "' not found.");
                }
            } else {
                LOG.error(Response.Status.NOT_FOUND.name() + ": invalid index type.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, Response.Status.NOT_FOUND.name() + ": invalid index type.");
            }

            LOG.debug("index " + indexname + " build status: " + results.get(RestConstants.DEFAULT_KEYWORD_PROGRESS).toString());
            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(results).build();
        } catch (OtsException e) {
            e.printStackTrace();
            return Response.status(e.getErrorCode() == OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT?Response.Status.FORBIDDEN : Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        } finally {
            if (getStatusMethod != null) {
                getStatusMethod.releaseConnection();
            }
            if (getYarnMethod != null) {
                getYarnMethod.releaseConnection();
            }
        }
    }

    @GET
    @Path("/{tablename}/{indexname}/display_columns")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfig(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname) {

        try {
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());

            LOG.debug("Get:" + indexname + " config!");
            String columns = IndexService.getConfig(userid, tenantid, tablename, indexname);
            if (null != columns) {
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(new IndexConfigModel(columns, 0L)).build();
            } else {
                LOG.error(Response.Status.NOT_FOUND.name() + ":" + indexname + " config is not exist.");
                return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorMode((long) OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX_PROFILE, Response.Status.NOT_FOUND.name() + ":" + indexname + " config is not exist.")).build();
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
    @Path("/{tablename}/{indexname}/display_columns")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response putConfig(@PathParam("tablename") String tablename, @PathParam("indexname") String indexname, String body) {

        try {
            long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());
            long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());

            LOG.debug("Put:" + indexname + " config!" + "\nContent:\n" + body);
            IndexConfigModel model = IndexConfigModel.toClass(body);

            IndexService.saveConfig(userid, tenantid, tablename, indexname, model.getColumns());
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
