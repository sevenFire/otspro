package com.baosight.xinsight.ots.rest.message.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.baosight.xinsight.kafka.CommonMessage;
import com.baosight.xinsight.kafka.MessageHandler;
import com.baosight.xinsight.ots.rest.permission.CachePermission;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;

public class PermissionUpdateHandlerImpl implements MessageHandler {
	private static final Logger LOG = Logger.getLogger(PermissionUpdateHandlerImpl.class);
	
	@Override
	public void execute(CommonMessage message) {
		// if the message is sent by self, do nothing
		try {
			if (message.getMsgFrom() != null 
					&& message.getMsgFrom().equals(ConfigUtil.getSystemIdentifier()))
				return;
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(message.getMsgContent());
			CachePermission.getInstance().batchRemove(rootNode.get("tableId").getValueAsText());
			LOG.debug((new StringBuilder()).append(ConfigUtil.getProjectName() + ":: ").append("All permission cache of table Id  :[").append(rootNode.get("tableId").getValueAsText()).append("] are labeled for updating !"));
		} catch (Exception e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

}
