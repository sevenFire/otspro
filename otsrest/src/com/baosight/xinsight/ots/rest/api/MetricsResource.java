package com.baosight.xinsight.ots.rest.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.service.MetricsService;
import com.baosight.xinsight.ots.rest.util.RegexUtil;

/**
 * 
 * @author huangming
 * 
 */
@Path("/metrics")
public class MetricsResource {
	private static final Logger LOG = Logger.getLogger(MetricsResource.class);

	@Context
	HttpServletRequest request;
	
	@GET
	@Path("/{tablename}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getbyTablename(@PathParam("tablename") String tablename) {
			
		try {
			if (!RegexUtil.isValidTableName(tablename)) {
				LOG.error(Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, Response.Status.FORBIDDEN.name() + ": tablename '" + tablename + "' contains illegal char.");
			}
			
			//String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
			//String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
			long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());	
			long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
			
			if (!MetricsService.isNamespaceExist(tenantid)) {			
				return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
						new ErrorMode((long) OtsErrorCode.EC_RDS_FAILED_QUERY_TENANT, Response.Status.NOT_FOUND.name() + ": tenantid '" + tenantid + "' is not exist.")).build();	
			}			
			else {
				return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(MetricsService.getMetricsInfo(userid, tablename)).build();
			}
		} catch (OtsException e) {
			e.printStackTrace();
			return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
		}	
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response get() {
		try {
			//String username = request.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
			//String tenant = request.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
			long userid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_USERID_KEY).toString());	
			long tenantid = Long.parseLong(request.getAttribute(CommonConstants.SESSION_TENANTID_KEY).toString());
			if (!MetricsService.isNamespaceExist(tenantid)) {
				return Response.status(Response.Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity(
						new ErrorMode((long) OtsErrorCode.EC_RDS_FAILED_QUERY_TENANT, Response.Status.NOT_FOUND.name() + ": tenantid '" + tenantid + "' is not exist.")).build();	
			}
			else {
				return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(MetricsService.getMetricsInfoByNamespace(tenantid, userid )).build();
			}
		} catch (OtsException e) {
			e.printStackTrace();
			return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(e.getErrorCode(), e.getMessage())).build();
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON).entity(new ErrorMode(500L, e.getMessage())).build();
		}	
	}
}

