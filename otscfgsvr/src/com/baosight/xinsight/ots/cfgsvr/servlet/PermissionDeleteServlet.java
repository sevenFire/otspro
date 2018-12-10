package com.baosight.xinsight.ots.cfgsvr.servlet;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.MessageHandlerFactory;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.cfgsvr.common.OtsCfgException;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.util.PrintWriterUtil;
import com.baosight.xinsight.utils.AasPermissionUtil;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.MessageBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
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
import java.util.HashMap;
import java.util.Map;

@WebServlet(urlPatterns = {"/servlet/delete_authority"})
public class PermissionDeleteServlet extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(PermissionDeleteServlet.class);

    /**
     * @author Zhang Yunlong Delete the permission of group about a specified table
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        this.doPost(request, response);
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setCharacterEncoding("UTF-8");
        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/json;charset=UTF-8");
        HttpServletRequest webRequest = request;
        HttpServletResponse webResponse = response;
        PrintWriter pw = webResponse.getWriter();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> results = new HashMap<String, Object>();
        String aasPermissionServiceAddrString = ConfigUtil.getInstance().getAuthServerAddr();
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
                    aasPermissionServiceAddrString, userInfo, OtsConstants.OTS_RESOURCE_MANAGE);
            if (null == permission || !permission.isGet()) {
                results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_PERMISSION_NO_PERMISSION_FAULT);
                PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
                LOG.debug("no permission fault errcode : " + mapper.writeValueAsString(results));
                return;
            }

            JsonNode rootNode = mapper.readTree(webRequest.getInputStream());
            LOG.debug("received json:" + rootNode.toString());
            String groupName = rootNode.get(RestConstants.AAS_GROUPE_NAME).getValueAsText();
            long instanceId = Long.parseLong(rootNode.get(RestConstants.AAS_TABLE_ID).toString());
            if (null == groupName || groupName.toString().isEmpty()) {
                LOG.error("Group Name can not be empty.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_GROUPNAME_EMPTY, RestConstants.ERROR_INFO_GROUPNAME_EMPTY);
            }
            if (instanceId <= 0) {
                LOG.error("Object ID must be a positive integer.");
                throw new OtsCfgException(OtsErrorCode.EC_OTS_PERMISSION_OBJECTID_EMPTY, RestConstants.ERROR_INFO_OBJECTSID_EMPTY);
            }

            String token = session.getAttribute(CommonConstants.SESSION_TOKEN_KEY).toString();
            HttpDelete httpDelete = new HttpDelete(new StringBuilder().append("http://").append(aasPermissionServiceAddrString).append("/api").append("/usergroup/").append(groupName)
                    .append("/resource?name=").append(AasPermissionUtil.OTS_INSTANCE_RESOURCE_PREFIX + instanceId).append("&service=").append(OtsConstants.OTS_SERVICE_NAME).toString());
            httpDelete.setHeader(CommonConstants.SESSION_TOKEN_KEY, token);
            HttpResponse httpResponse = new DefaultHttpClient().execute(httpDelete);
            HttpEntity responseEntity = httpResponse.getEntity();
            if (responseEntity != null) {
                InputStream in = responseEntity.getContent();
                String strResult = AasPermissionUtil.convertStreamToString(in);
                JsonNode deleteResult = mapper.readTree(strResult);
                LOG.debug("Delete authority result:" + deleteResult.get(OtsConstants.OTS_ERROR_CODE).toString());

                //clear local a specifid cache permision
                CachePermission.getInstance().batchRemove(rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsText());
                results.put(OtsConstants.OTS_ERROR_CODE, deleteResult.get(OtsConstants.OTS_ERROR_CODE));

                //send the kafka message for removing a specifid cache permission on cluster
                MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC)
                        .sendData(tenantId, MessageBuilder.buildPermissionUpdateMessage(rootNode.get(RestConstants.AAS_TABLE_ID).getValueAsLong()));
            }
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
            LOG.debug("The permission action of group: " + groupName + "has been delete successfully!");
        } catch (OtsCfgException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            results.clear();
            results.put(OtsConstants.OTS_ERROR_CODE, e.getErrcode());
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            results.clear();
            results.put(OtsConstants.OTS_ERROR_CODE, OtsErrorCode.EC_OTS_DELETE_PERMISSION_FAILED);
            PrintWriterUtil.PrintAndClose(pw, mapper.writeValueAsString(results));
        }
        LOG.debug("response cancelResponse:" + results.toString());
    }
}
