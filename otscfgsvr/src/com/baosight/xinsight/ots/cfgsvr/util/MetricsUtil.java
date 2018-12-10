package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.baosight.xinsight.ots.cfgsvr.timer.MetricsJob;

public class MetricsUtil {
	private static final Logger LOG = Logger.getLogger(MetricsUtil.class);

	private static Scheduler scheduler = null;
	private static JobDetail metricsJob = null;
	private static CronTrigger metricsTrigger = null;
	
	public static void doWork() {	
		LOG.debug("MetricsUtil doWork begin!");
		
		//job
	  	metricsJob = JobBuilder.newJob(MetricsJob.class).withIdentity("metricsJob", ConfigUtil.getProjectName()).build();
 
		try {
			//trigger
			String metric_schedule = ConfigUtil.getInstance().getValue("metric_schedule");
			String row_count_schedule = ConfigUtil.getInstance().getValue("row_count_schedule");
			metricsTrigger = TriggerBuilder.newTrigger().withIdentity("metricTrigger", ConfigUtil.getProjectName())
					.withSchedule(CronScheduleBuilder.cronSchedule(metric_schedule)).build();
			LOG.debug("metricTrigger was: " + metric_schedule);
			LOG.debug("rowCountTrigger was: " + row_count_schedule);
 
			//schedule it
			scheduler = new StdSchedulerFactory().getScheduler();
	    	scheduler.scheduleJob(metricsJob, metricsTrigger);
	    	scheduler.start();
		} catch (SchedulerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean checkWork() {		
		try {
			if (scheduler == null) {				
				scheduler = new StdSchedulerFactory().getScheduler();
				scheduler.scheduleJob(metricsJob, metricsTrigger);
		    	scheduler.start();
		    	return true;
			}
			
			if (scheduler.isInStandbyMode() || scheduler.isShutdown()) {
		    	scheduler.start();		    	
			}
			
			return scheduler.isStarted();
			
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
			
		return false;		
	}
	
	public static void finiWork() {
		LOG.debug("MetricsUtil doWork end!");
		
		try {
			if (scheduler != null && !scheduler.isShutdown()) {
				scheduler.shutdown();
			}
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}
}
