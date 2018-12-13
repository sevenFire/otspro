package com.baosight.xinsight.ots.rest.util;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.CommonMessage;
import com.baosight.xinsight.model.UserInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MessageBuilder {
	private final static Logger LOG = Logger.getLogger(MessageBuilder.class);

	public static CommonMessage buildPermissionMessage(UserInfo userInfo, long tableId, boolean readPermission, boolean writePermission,
													   boolean permissionFlag, String currentTime) {
		CommonMessage message = null;
		try {
			message = new CommonMessage();
			message.setMsgId(CommonConstants.OTS_PERMISSION_MESSAGE_ID);
			message.setMsgFrom(ConfigUtil.getSystemIdentifier());
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> builder = new HashMap<String, String>();		
			builder.put("userId", String.valueOf(userInfo.getUserId()));
			builder.put("tenantId", String.valueOf(userInfo.getTenantId()));
			builder.put("tableId", String.valueOf(tableId));
			builder.put("tenant", String.valueOf(userInfo.getTenantName()));
			builder.put("username", String.valueOf(userInfo.getUserName()));
			builder.put("readPermission", String.valueOf(readPermission));
			builder.put("writePermission", String.valueOf(writePermission));
			builder.put("permissionFlag", String.valueOf(permissionFlag));
			builder.put("currentTime", currentTime);
			message.setMsgContent(mapper.writeValueAsString(builder));
			LOG.debug("buildPermissionMessage" + ConfigUtil.getProjectName() + ": " + mapper.writeValueAsString(builder));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (JsonMappingException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
		return message;
	}
	
	public static CommonMessage buildPermissionUpdateMessage( long tableId){
		CommonMessage message = null;
		try {
			message = new CommonMessage();
			message.setMsgId(CommonConstants.OTS_PERMISSION_UPDATE_MESSAGE_ID);
			message.setMsgFrom(ConfigUtil.getSystemIdentifier());
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> builder = new HashMap<String, String>();
			builder.put("tableId", String.valueOf(tableId));
			message.setMsgContent(mapper.writeValueAsString(builder));
			LOG.debug("buildPermissionUpdateMessage" + ConfigUtil.getProjectName() + ": " + mapper.writeValueAsString(builder));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (JsonMappingException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
		return message;
	}
	
	public static CommonMessage buildPermissionRemoveMessage( String removeKey){
		CommonMessage message = null;
		try {
			message = new CommonMessage();
			message.setMsgId(CommonConstants.OTS_PERMISSION_REMOVE_MESSAGE_ID);
			message.setMsgFrom(ConfigUtil.getSystemIdentifier());
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> builder = new HashMap<String, String>();
			builder.put("removeKey", removeKey);
			message.setMsgContent(mapper.writeValueAsString(builder));
			LOG.debug("buildPermissionRemoveMessage" + ConfigUtil.getProjectName() + ": " + mapper.writeValueAsString(builder));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (JsonMappingException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
		return message;
	}

	public static CommonMessage buildConfigMessage(int op, Long tenantid, Long userid, String tablename) {
		CommonMessage message =null;
		try {
			message = new CommonMessage();
			message.setMsgId(CommonConstants.OTS_CONFIG_MESSAGE_ID);
			message.setMsgFrom(ConfigUtil.getSystemIdentifier());
			Map<String, String> builder = new HashMap<String,String>();
			ObjectMapper mapper = new ObjectMapper();
			builder.put("op", String.valueOf(op));
			builder.put("tenantId", String.valueOf(tenantid));
			builder.put("userId", String.valueOf(userid));
			builder.put("tableName", tablename);		

			message.setMsgContent(mapper.writeValueAsString(builder));
			LOG.debug("buildConfigMessage" + ConfigUtil.getProjectName() + ": " + mapper.writeValueAsString(builder));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (JsonMappingException e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		} catch (IOException e) {		
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
		return message;
	}
}
