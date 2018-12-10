package com.baosight.xinsight.ots.cfgsvr.servlet;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.util.PrintWriterUtil;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet(urlPatterns = { "/servlet/group_list" })
public class GroupListsServlet extends HttpServlet {
	private static final Logger LOG = Logger.getLogger(GroupListsServlet.class);

	/**
	 * @author Zhang Yunlong Obtain all groups contained by the current tenant
	 * @return all groups of current tenant.
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void init() {
		LOG.debug("The Ots Permission Servlet is operated !  ");
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("UTF-8");
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/json;charset=UTF-8");
		HttpServletRequest webRequest = request;
		HttpServletResponse webResponse = response;
		PrintWriter pw = webResponse.getWriter();
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			HttpSession session = webRequest.getSession(false);
			String tenant = session.getAttribute(CommonConstants.SESSION_TENANT_KEY).toString();
			String userName = session.getAttribute(CommonConstants.SESSION_USERNAME_KEY).toString();
			long tenantId = Long.parseLong(session.getAttribute("tenantId").toString());
			long userId = Long.parseLong(session.getAttribute("tenantId").toString());
			PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
			userInfo.setTenantName(tenant);
			userInfo.setUserName(userName);
			userInfo.setTenantId(tenantId);
			userInfo.setUserId(userId);
			userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
			AasPermissionUtil.Permission permission = AasPermissionUtil.getPermissionByResource(
					ConfigUtil.getInstance().getAuthServerAddr()
					, userInfo
					, OtsConstants.OTS_RESOURCE_MANAGE);
			if (null == permission || !permission.isGet()) {
				results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
				PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
				LOG.debug("no permission fault errcode : " + mapper.writeValueAsString(results));
				return;
			}
			// query group list with offest = 0 and limit = 200
			HttpGet httpGets = new HttpGet((new StringBuilder()).append("http://").append(ConfigUtil.getInstance().getAuthServerAddr()).append("/api").append("/usergroup?limit=200&offset=0").toString());
			httpGets.setHeader(CommonConstants.SESSION_TOKEN_KEY, (String) session.getAttribute(CommonConstants.SESSION_TOKEN_KEY));
			HttpResponse httpResponse = new DefaultHttpClient().execute(httpGets);
			HttpEntity entity = httpResponse.getEntity();
			List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
			if (entity != null) {
				InputStream in = entity.getContent();
				String strGroups = AasPermissionUtil.convertStreamToString(in);
				LOG.debug(strGroups);
				JsonNode rootNode = mapper.readTree(strGroups);
				if (rootNode.get(OtsConstants.OTS_ERROR_INFO) != null) {
					results.put(OtsConstants.OTS_ERROR_CODE, rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsText());
					results.put(OtsConstants.OTS_ERROR_INFO, rootNode.get(OtsConstants.OTS_ERROR_INFO).getValueAsText());
					LOG.error(
							"response elements:" + rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsText() + ".  Error Reason :"
									+ rootNode.get(OtsConstants.OTS_ERROR_INFO).getValueAsText());
					PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
					return;
				}
				JsonNode groupArray = rootNode.path(RestConstants.AAS_GROUPS);
				for (int i = 0; i < groupArray.size(); i++) {
					Map<String, String> resultsNode = new HashMap<String, String>();
					JsonNode groupNode = groupArray.get(i);
					if (groupNode.has(RestConstants.AAS_NAME)) {
						resultsNode.put(RestConstants.AAS_NAME, groupNode.get(RestConstants.AAS_NAME).getValueAsText());
					}
					resultList.add(resultsNode);
					LOG.debug("response elements:" + resultList.toString());
				}
				results.put(OtsConstants.OTS_ERROR_CODE, rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsText());
				results.put(OtsConstants.OTS_TOTAL_COUNT, rootNode.get(OtsConstants.OTS_TOTAL_COUNT).getValueAsText());
				results.put(RestConstants.AAS_GROUPS, resultList);
				PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
				LOG.debug("The Group List displayment operation is finished!");
			}
		} catch (Exception e) {
			results.clear();
			results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_PERMISSION_GET_GROUP_LIST_FAILED);
			PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doGet(request, response);
	}

}
