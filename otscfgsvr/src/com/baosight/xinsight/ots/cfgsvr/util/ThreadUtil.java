package com.baosight.xinsight.ots.cfgsvr.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

public class ThreadUtil {
	private static final Logger LOG = Logger.getLogger(ThreadUtil.class);

	public static ExecutorService executor = null;
	
	public static void init() {	
		LOG.debug("BackupUtil init new thread pool!");
		
		shutdown();		
		executor = Executors.newSingleThreadExecutor();	
	}
	
	public static Future<?> submit(Runnable task) {
		Future<?> future = null;
		if (executor == null || executor.isShutdown()) {
			executor = Executors.newSingleThreadExecutor();	
		}
		future = executor.submit(task);
		return future;
	}
	
	public static void checkWork() {
		
	}
	
	public static void shutdown() {
		if (executor != null && !executor.isShutdown()) {
			executor.shutdown();
		}		
	}
}
