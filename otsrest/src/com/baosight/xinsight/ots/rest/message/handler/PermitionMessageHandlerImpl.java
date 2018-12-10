package com.baosight.xinsight.ots.rest.message.handler;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.CommonMessage;
import com.baosight.xinsight.kafka.MessageHandler;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class PermitionMessageHandlerImpl implements MessageHandler {
	private static final Logger LOG = Logger.getLogger(PermitionMessageHandlerImpl.class);
	
	@Override
	public void execute(CommonMessage message) {
		// if the message is sent by self, do nothing
		try {
			if (message.getMsgFrom() != null 
					&& message.getMsgFrom().equals(ConfigUtil.getSystemIdentifier()))
				return;

			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(message.getMsgContent());
			PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
			userInfo.setTenantName( rootNode.get("tenant").getValueAsText());
			userInfo.setTenantId(rootNode.get("tenantId").getValueAsLong());
			userInfo.setUserName(rootNode.get("username").getValueAsText());
			userInfo.setUserId(rootNode.get("userId").getValueAsLong());
			userInfo.setServiceName(rootNode.get("serviceName").getValueAsText());
			CachePermission.getInstance().getPermission(
					userInfo, 
					rootNode.get("tableId").getValueAsLong(), 
					rootNode.get("readPermission").getValueAsBoolean(), 
					rootNode.get("writePermission").getValueAsBoolean(), 
					rootNode.get("permissionFlag").getValueAsBoolean(), 
					rootNode.get("currentTime").getValueAsText());
			
			LOG.debug((new StringBuilder()).append(ConfigUtil.getProjectName() + ":: ").append("Permission cache of key of ots: ").append(rootNode.get("userId").getValueAsLong()).append(CommonConstants.DEFAULT_SINGLE_UNLINE_SPLIT)
					.append(rootNode.get("tableId").getValueAsLong()).append(" tenantId :").append(rootNode.get("tenantId").getValueAsLong()).append(" it's permission updated as  RP=").append(rootNode.get("readPermission").getValueAsBoolean()).append(" WP=")
					.append(rootNode.get("writePermission").getValueAsBoolean()).append(" PF= ").append(rootNode.get("permissionFlag").getValueAsBoolean()).toString());
			
		} catch (Exception e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
	}
}
