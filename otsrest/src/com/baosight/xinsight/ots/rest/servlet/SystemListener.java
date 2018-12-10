package com.baosight.xinsight.ots.rest.servlet;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.kafka.MessageConsumerThread;
import com.baosight.xinsight.kafka.MessageHandlerFactory;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.rest.message.handler.ConfigMessageHandlerImpl;
import com.baosight.xinsight.ots.rest.message.handler.PermissionClearHandlerImpl;
import com.baosight.xinsight.ots.rest.message.handler.PermissionUpdateHandlerImpl;
import com.baosight.xinsight.ots.rest.message.handler.PermitionMessageHandlerImpl;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.utils.ImmolateUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class SystemListener implements ServletContextListener {
	private final static Logger LOG = Logger.getLogger(SystemListener.class);
	private ImmolateUtil immolateUtil = null;
	private static MessageConsumerThread consumerThread = null;

	@Override
	public void contextDestroyed(ServletContextEvent event) {	
		try {
			PermissionUtil.GetInstance().stop();
			
			consumerThread.finalize();
			MessageHandlerFactory.getMessageProducer(CommonConstants.OTS_CONFIG_TOPIC).finalize();
			
			ConfigUtil.getInstance().stop();	
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
		
		immolateUtil.deregisterDriver();
		immolateUtil.immolate();		
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		ConfigUtil.getInstance();		
		PermissionUtil.init(OtsConstants.OTS_SERVICE_OTSREST_NAME);
		PermissionUtil.GetInstance();
		immolateUtil = new ImmolateUtil(false);

		try {    		 
			consumerThread = MessageHandlerFactory.getMessageConsumerThread(CommonConstants.OTS_CONFIG_TOPIC, ConfigUtil.getSystemIdentifier());
			consumerThread.start();
			
			consumerThread.registerMsgHandler(CommonConstants.OTS_PERMISSION_MESSAGE_ID, new PermitionMessageHandlerImpl());
			consumerThread.registerMsgHandler(CommonConstants.OTS_CONFIG_MESSAGE_ID, new ConfigMessageHandlerImpl());
			consumerThread.registerMsgHandler(CommonConstants.OTS_PERMISSION_UPDATE_MESSAGE_ID, new PermissionUpdateHandlerImpl());
			consumerThread.registerMsgHandler(CommonConstants.OTS_PERMISSION_REMOVE_MESSAGE_ID, new PermissionClearHandlerImpl());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}	
	}
}
