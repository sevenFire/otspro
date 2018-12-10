package com.baosight.xinsight.ots.cfgsvr.servlet;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.MessageHandlerFactory;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.OtsCfgException;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.util.PrintWriterUtil;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.MessageBuilder;
import com.baosight.xinsight.utils.AasPermissionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
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

@WebServlet(urlPatterns = {"/servlet/add_authority"})
public class PermissionAddServlet extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(PermissionAddServlet.class);


    /**
     * @author Zhang Yunlong add permission operation
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        LOG.debug("The Ots Permission Servlet is operated !  ");
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        this.doPost(request, response);
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/json;charset=UTF-8");
        HttpServletRequest webRequest = request;
        HttpServletResponse webResponse = response;
        PrintWriter pw = webResponse.getWriter();
        Map<String, Object> results = new HashMap<String, Object>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            HttpSession session = webRequest.getSession(false);
            String addr = ConfigUtil.getInstance().getAuthServerAddr();
            String token = session.getAttribute(CommonConstants.SESSION_TOKEN_KEY).toString();
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
                PrintWriterUtil.PrintAndClose(pw, results.toString());
                LOG.debug("no permission fault errcode : " + results.toString());
                return;
            }
            JsonNode rootNode = mapper.readTree(webRequest.getInputStream());
            LOG.debug("received json:" + rootNode.toString());
            String groupName = rootNode.get(RestConstants.AAS_GROUPE_NAME).getValueAsText();
            if (null == groupName || groupName.toString().isEmpty()) {
                LOG.error("Group name List can not be empty.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_GROUPNAME_EMPTY, RestConstants.ERROR_INFO_GROUPNAME_EMPTY);
            }
            if (null == rootNode.get(RestConstants.AAS_TABLE_ID) || rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong() <= 0) {
                LOG.error("Object ID must be a positive integer.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_OBJECTID_EMPTY, RestConstants.ERROR_INFO_OBJECTSID_EMPTY);
            }
            if (null == rootNode.get(RestConstants.AAS_DISPLAY_RESOURCE_NAME)
                    || rootNode.get(RestConstants.AAS_DISPLAY_RESOURCE_NAME).getValueAsText().isEmpty()
                    || null == rootNode.get(RestConstants.AAS_RELEVANT_SERVICE) || rootNode.get(RestConstants.AAS_RELEVANT_SERVICE).toString().isEmpty()) {
                LOG.error("Display name and Service name can not be empty.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_PARAMERTERS_NOT_COMPLETE, RestConstants.ERROR_INFO_DISNAME_OR_SERNAME_EMPTY);
            }
            /*
            * generate the permission resource name of ots table
            */
            String name = AasPermissionUtil.OTS_INSTANCE_RESOURCE_PREFIX + rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong();
            // registration operation
            results.put(OtsConstants.OTS_ERROR_CODE, AasPermissionUtil.registerResource(addr, userInfo, name, name, rootNode.get(RestConstants.AAS_DISPLAY_RESOURCE_NAME).getValueAsText()));
            // permission operation
            Map<String, List<Map<String, Object>>> addPermission = new HashMap<String, List<Map<String, Object>>>();
            Map<String, Boolean> permissionAction = new HashMap<String, Boolean>();
            Map<String, Object> resourceElement = new HashMap<String, Object>();
            List<Map<String, Object>> resourcesList = new ArrayList<Map<String, Object>>();
            resourceElement.put(RestConstants.AAS_NAME, name);
            resourceElement.put(RestConstants.AAS_SERVICE, rootNode.get(RestConstants.AAS_RELEVANT_SERVICE).getValueAsText());
            JsonNode permissionJson = rootNode.get(RestConstants.AAS_RELEVANT_AUTHORITY);
            if (permissionJson.has("GET")) {
                permissionAction.put("GET", permissionJson.get("GET").getValueAsBoolean());
            }
            if (permissionJson.has("POST")) {
                permissionAction.put("POST", permissionJson.get("POST").getValueAsBoolean());
            }
            if (permissionJson.has("PUT")) {
                permissionAction.put("PUT", permissionJson.get("PUT").getValueAsBoolean());
            }
            if (permissionJson.has("DELETE")) {
                permissionAction.put("DELETE", permissionJson.get("DELETE").getValueAsBoolean());
            }
            resourceElement.put("action", permissionAction);
            resourcesList.add(resourceElement);
            addPermission.put(RestConstants.AAS_RESOURCES, resourcesList);
            LOG.debug("Authority Information:" + addPermission.toString());
            HttpPut httpPut = new HttpPut((new StringBuilder()).append("http://").append(addr).append("/api").append("/usergroup/").append(groupName).append("/resource").toString());
            LOG.debug("received json:" + addPermission.toString());
            httpPut.setHeader(CommonConstants.SESSION_TOKEN_KEY, token);
            StringEntity putEntity = new StringEntity(mapper.writeValueAsString(addPermission), "UTF-8");
            putEntity.setContentEncoding("UTF-8");
            putEntity.setContentType("application/json");
            httpPut.setEntity(putEntity);
            LOG.debug("received HttpPut:" + httpPut);
            HttpResponse httpResponse = new DefaultHttpClient().execute(httpPut);
            HttpEntity resEntity = httpResponse.getEntity();
            if (resEntity != null) {
                InputStream in = resEntity.getContent();
                String strResponse = AasPermissionUtil.convertStreamToString(in);
                JsonNode authNode = mapper.readTree(strResponse);
                LOG.debug("received json:" + authNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong());
                results.put(OtsConstants.OTS_ERROR_CODE, authNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong());
                if (authNode.get(OtsConstants.OTS_ERROR_CODE).getValueAsLong() == 0) {
                    ConfigUtil.getInstance().getOtsAdmin().setTablePermittion(rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong());

                    //save permision into local cache
                    CachePermission.getInstance().batchRemove(String.valueOf(rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong()));

                    //send kafka message about updating cache permission
                    MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC)
                            .sendData(tenantId, MessageBuilder.buildPermissionUpdateMessage(rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong()));
                } else {
                    results.put(OtsConstants.OTS_ERROR_INFO, authNode.get(OtsConstants.OTS_ERROR_INFO).getValueAsText());
                }
            }
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
            LOG.debug("The permission has been added!" + resourcesList.toString());

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
}
