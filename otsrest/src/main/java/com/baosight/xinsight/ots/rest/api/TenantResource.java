package com.baosight.xinsight.ots.rest.api;

import com.baosight.xinsight.auth.AuthManager;
import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.OTSTable;
import com.baosight.xinsight.ots.common.index.Column;
import com.baosight.xinsight.ots.common.index.IndexInfo;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.model.TokenQueryModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.TableCreateModel;
import com.baosight.xinsight.ots.rest.service.TableService;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

@Path("/")
public class TenantResource {
    private static final Logger LOG = Logger.getLogger(TenantResource.class);

    @Context
    UriInfo uriInfo;
    @Context
    HttpServletRequest request;

    @POST
    @Path("/tenant/init")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(String body) {
        try {
            LOG.debug("Tenant Initialize Post:" + body);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(body);
            String tenant_id = rootNode.path(CommonConstants.SESSION_TENANTID_KEY).getValueAsText();
            
    	    if (tenant_id == null || tenant_id.isEmpty()) {
                LOG.error("Tenant Initialize invalid pararms: tenant_id=" + tenant_id);
                throw new OtsException(RestErrorCode.EC_OTS_REST_INIT_INNERTABLE, "Tenant Initialize invalid pararms.");
			}

            /*******************************create pds inner table**********************************/
            long namespace_PDS = CommonConstants.DEFAULT_PDS_OTSNAMESPACE;
            String tablename_PDS = CommonConstants.PDS_INNER_TABLE_PREFIX + tenant_id;
            if (!TableService.innerHbaseTableExist(0, namespace_PDS, tablename_PDS)) {
                TableCreateModel pds_model = new TableCreateModel(1, 1, false, 0, "pds init");
                pds_model.setKeyType(0);
                pds_model.setHashKeyType(0);
                pds_model.setRangeKeyType(-1);
                pds_model.setReplication(0);//!!important

                LOG.debug("Post init PDS Table:" + tablename_PDS + "\nContent:\n" + pds_model.toString());
                TableService.innerHbaseCreateTable(0, namespace_PDS, tablename_PDS, pds_model);
            }

            /*******************************create pds_alarm inner table**********************************/
            String tablename_PDS_alarm = CommonConstants.PDS_ALARM_INNER_TABLE_PREFIX + tenant_id;
            if (!TableService.innerHbaseTableExist(0, namespace_PDS, tablename_PDS_alarm)) {
                TableCreateModel pds_alarm_model = new TableCreateModel(1, 1, false, 0, "pds_alarm init");
                pds_alarm_model.setKeyType(0);
                pds_alarm_model.setHashKeyType(0);
                pds_alarm_model.setRangeKeyType(-1);
                pds_alarm_model.setReplication(1);//!!important

                LOG.debug("Post init PDS Alarm Table:" + tablename_PDS_alarm + "\nContent:\n" + pds_alarm_model.toString());
                TableService.innerHbaseCreateTable(0, namespace_PDS, tablename_PDS_alarm, pds_alarm_model);
                
                /*********************************create pds_alarm inner index********************************/
                String pds_alarm_solrindex = CommonConstants.PDS_ALARM_INNER_INDEX_NAME;
                IndexInfo solr_index_model = new IndexInfo();
                solr_index_model.setName(pds_alarm_solrindex);
                solr_index_model.setStartKey(null);
                solr_index_model.setEndKey(null);
                solr_index_model.setPattern((char) (OtsConstants.OTS_INDEX_PATTERN_ONLINE));
                solr_index_model.setShardNum(Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_SHARD_NUM, "3")));
                solr_index_model.setReplicationNum(Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_REPLICATION_FACTOR, "1")));
                solr_index_model.setMaxShardNumPerNode(Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_MAX_SHARDS_PERNODE, "3")));
                List<Column> columns = new ArrayList<>();
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_TIME, CommonConstants.PDS_ALARM_INNER_INDEX_COL_TIME_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_PRIORITY, CommonConstants.PDS_ALARM_INNER_INDEX_COL_PRIORITY_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_STATUS, CommonConstants.PDS_ALARM_INNER_INDEX_COL_STATUS_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_VALUE, CommonConstants.PDS_ALARM_INNER_INDEX_COL_VALUE_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_SHOWN, CommonConstants.PDS_ALARM_INNER_INDEX_COL_SHOWN_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_ALARM_TIMES, CommonConstants.PDS_ALARM_INNER_INDEX_COL_ALARM_TIMES_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_TENANT_ID, CommonConstants.PDS_ALARM_INNER_INDEX_COL_TENANT_ID_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_ALARM_TYPE, CommonConstants.PDS_ALARM_INNER_INDEX_COL_ALARM_TYPE_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_OBJECT_ID, CommonConstants.PDS_ALARM_INNER_INDEX_COL_OBJECT_ID_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_ATTRIBUTE_ID, CommonConstants.PDS_ALARM_INNER_INDEX_COL_ATTRIBUTE_ID_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_RECOVER_TIME, CommonConstants.PDS_ALARM_INNER_INDEX_COL_RECOVER_TIME_SOLRTYPE));
                columns.add(new Column(CommonConstants.PDS_ALARM_INNER_INDEX_COL_CONFIRM_TIME, CommonConstants.PDS_ALARM_INNER_INDEX_COL_CONFIRM_TIME_SOLRTYPE));
                solr_index_model.setColumns(columns);
                LOG.debug("init PDS Alarm Table solr index:" + tablename_PDS_alarm);
                
                OTSTable pds_alarm_table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(0, namespace_PDS, tablename_PDS_alarm);
                pds_alarm_table.innerCreateSolrIndex(solr_index_model);                              
            }             
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }

        // successful
        return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(0L)).build();
    }

    @GET
    @Path("/token")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get() {
        try {

            String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
            String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
            String password = request.getAttribute(CommonConstants.IN_SESSION_PASSWORD_KEY).toString();

            TokenQueryModel model = new TokenQueryModel();
            String token = AuthManager.login(ConfigUtil.getInstance().getAuthServerAddr(), tenant, username, password);
            model.setToken(token);
            if (token == null) {
                model.setErrcode(RestErrorCode.EC_OTS_REST_QUERY_TOKEN);
            } else {
                model.setErrcode(0L);
            }

            return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(model).build();

        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
        }
    }
}

