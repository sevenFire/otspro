package com.baosight.xinsight.ots.cfgsvr.servlet;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.OtsCfgException;
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
import java.util.*;

@WebServlet(urlPatterns = {"/servlet/group_authority_list"})
public class PermittdGroupsListServlet extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(PermittdGroupsListServlet.class);

    /**
     * @author Zhang Yunlong Display all permission action status of groups
     * relate to a specified table
     * @return all groups and relevant permission action about a specified
     * table.
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        LOG.debug("The Ots Manage Instance Servlet is operated !  ");
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/json;charset=UTF-8");
        HttpServletRequest webRequest = request;
        HttpServletResponse webResponse = response;
        Map<String, Object> results = new HashMap<String, Object>();
        ObjectMapper mapper = new ObjectMapper();
        PrintWriter pw = webResponse.getWriter();
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
                    ConfigUtil.getInstance().getAuthServerAddr(), userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
            if (null == permission || !permission.isGet()) {
                results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
                PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
                LOG.debug("no permission fault errcode : " + mapper.writeValueAsString(results).toString());
                return;
            }
            Long groupPage = Long.parseLong(webRequest.getParameter("page"));
            Long groupLimit = Long.parseLong(webRequest.getParameter("limit"));
            long groupOffset = (groupPage - 1) * groupLimit;
            String tableId = AasPermissionUtil.OTS_INSTANCE_RESOURCE_PREFIX + webRequest.getParameter(RestConstants.AAS_TABLE_ID);
            if (null == tableId || tableId.isEmpty()) {
                LOG.error("checkService or  checkIdAndName can not be empty.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_OBJECTID_EMPTY, RestConstants.ERROR_INFO_GROUPNAME_EMPTY);
            }
            // first query account more than one of general limit, because of admin group need to be dismiss at frontend
            // and then, each offset need to be add one.
            if (0 == groupOffset) {
                groupLimit = groupLimit + 1;
            } else {
                groupOffset = groupOffset + 1;
            }
            HttpGet httpGet = new HttpGet((new StringBuilder()).append("http://").append(ConfigUtil.getInstance().getAuthServerAddr()).append("/api").append("/resource/")
                    .append(OtsConstants.OTS_SERVICE_NAME).append("/").append(tableId).append("/usergroup?offset=").append(groupOffset).append("&limit=").append(groupLimit).toString());
            httpGet.setHeader(CommonConstants.SESSION_TOKEN_KEY, (String) session.getAttribute(CommonConstants.SESSION_TOKEN_KEY));
            HttpResponse httpResponse = new DefaultHttpClient().execute(httpGet);
            HttpEntity entity = httpResponse.getEntity();
            List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
            if (entity != null) {
                InputStream in = entity.getContent();
                String strGroupPermission = AasPermissionUtil.convertStreamToString(in);
                LOG.debug("received json:" + strGroupPermission);
                JsonNode rootNode = mapper.readTree(strGroupPermission);
                if (rootNode.get(OtsConstants.OTS_ERROR_INFO) != null) {
                    results.put(OtsConstants.OTS_ERROR_CODE, rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong());
                    results.put(OtsConstants.OTS_ERROR_INFO, rootNode.get(OtsConstants.OTS_ERROR_INFO).getValueAsText());
                    LOG.error(
                            "response elements:" + rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong() + ".  Error Reason :"
                                    + rootNode.path(OtsConstants.OTS_ERROR_INFO).getValueAsText());
                    PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
                    return;
                }
                results.put(OtsConstants.OTS_TOTAL_COUNT, String.valueOf(rootNode.get(OtsConstants.OTS_TOTAL_COUNT).getValueAsLong() - 1));
                JsonNode groupArray = rootNode.path(RestConstants.AAS_GROUPS);
                for (int i = 0; i < groupArray.size(); i++) {
                    Map<String, String> mapNode = new HashMap<String, String>();
                    JsonNode groupNode = groupArray.get(i);
                    if (groupNode.get(RestConstants.AAS_NAME).getValueAsText().equals(RestConstants.AAS_ADMAIN_GROUP)) {
                        continue;
                    }
                    mapNode.put(RestConstants.AAS_NAME, groupNode.get(RestConstants.AAS_NAME).getValueAsText());
                    JsonNode strAction = groupNode.get(RestConstants.AAS_PERMISSION_ACTION);
                    List<String> list = new LinkedList<String>();
                    for (int j = 0; j < strAction.size(); j++) {
                        list.add(strAction.get(j).getValueAsText());
                    }
                    mapNode.put("READ", list.contains("GET") ? "TRUE" : "FALSE");
                    mapNode.put("MANAGE", list.contains("POST") ? "TRUE" : "FALSE");
                    resultList.add(mapNode);
                    LOG.debug("response elements:" + resultList.toString());
                }
                results.put(OtsConstants.OTS_ERROR_CODE, rootNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong());
                results.put(RestConstants.AAS_GROUPS, resultList);
                PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
                LOG.debug("The Group & Permission status List displayment operation is finished!");
            }
        } catch (OtsCfgException e) {
            results.clear();
            results.put(OtsConstants.OTS_ERROR_CODE, e.getErrcode());
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (Exception e) {
            results.clear();
            results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_PERMISSION_GET_GROUP_LIST_FAILED);
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     * response)
     */
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        this.doGet(request, response);
    }
}
