package com.baosight.xinsight.ots.rest.message.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.baosight.xinsight.kafka.CommonMessage;
import com.baosight.xinsight.kafka.MessageHandler;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.TableConfigUtil;


/**
 *
 *@ Author Zhang Yunlong
 */
public class ConfigMessageHandlerImpl implements MessageHandler {
	private static final Logger LOG = Logger.getLogger(ConfigMessageHandlerImpl.class);

	@Override
	public void execute(CommonMessage message) {
		try {
			if (message.getMsgFrom() != null 
					&& message.getMsgFrom().equals(ConfigUtil.getSystemIdentifier()))
				return;
			
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(message.getMsgContent());
			if (rootNode.get("op").getValueAsInt() == RestConstants.OTS_WEBOP_ADD) {
				
				LOG.debug(new StringBuilder().append(ConfigUtil.getProjectName() + ":: ").append("receive valid message: add table(").append(rootNode.get("tableName").getValueAsText()).append(") cache for user(")
						.append(rootNode.get("userId").getValueAsLong()).append(") of tenant(").append(rootNode.get("tenantId").getValueAsLong()).append(")").toString());
				
				TableConfigUtil.syncAdd(rootNode.get("tenantId").getValueAsLong(), rootNode.get("userId").getValueAsLong(), rootNode.get("tableName").getValueAsText());
			} else if (rootNode.get("op").getValueAsInt() == RestConstants.OTS_WEBOP_DELETE) {
				
				LOG.debug(new StringBuilder().append(ConfigUtil.getProjectName() + ":: ").append("receive valid message: delete table(").append(rootNode.get("tableName").getValueAsText()).append(") cache for user(")
						.append(rootNode.get("userId").getValueAsLong()).append(") of tenant(").append(rootNode.get("tenantId").getValueAsLong()).append(")").toString());
				
				TableConfigUtil.syncDel(rootNode.get("tenantId").getValueAsLong(), rootNode.get("userId").getValueAsLong(), rootNode.get("tableName").getValueAsText());
			} else if (rootNode.get("op").getValueAsInt() == RestConstants.OTS_WEBOP_UPDATE) {
				
				LOG.debug(new StringBuilder().append(ConfigUtil.getProjectName() + ":: ").append("receive valid message: update table(").append(rootNode.get("tableName").getValueAsText()).append(") cache for user(")
						.append(rootNode.get("userId").getValueAsLong()).append(") of tenant(").append(rootNode.get("tenantId").getValueAsLong()).append(")").toString());
				
				TableConfigUtil.syncUpdate(rootNode.get("tenantId").getValueAsLong(), rootNode.get("userId").getValueAsLong(), rootNode.get("tableName").getValueAsText());
			}
		} catch (Exception e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
	}
}
