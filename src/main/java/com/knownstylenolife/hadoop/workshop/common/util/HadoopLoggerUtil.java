package com.knownstylenolife.hadoop.workshop.common.util;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;

import com.knownstylenolife.hadoop.workshop.common.consts.ConfigurationConst;


public class HadoopLoggerUtil {

	public static void setLogLevel(org.apache.commons.logging.Log log, Configuration configuration) {
		String logLevel = configuration.get(ConfigurationConst.LOG_LEVEL);
		if(logLevel != null) {
			HadoopLoggerUtil.setLogLevel(log, org.apache.log4j.Level.toLevel(logLevel));
		}
	}
	
	public static void setLogLevel(org.apache.commons.logging.Log log, String level) {
		HadoopLoggerUtil.setLogLevel(log, org.apache.log4j.Level.toLevel(level));
	}
	
	public static void setLogLevel(org.apache.commons.logging.Log log, org.apache.log4j.Level level) {
		if(log instanceof Log4JLogger) {
			log.info("Set Log4JLogger log level to " + level);
			org.apache.log4j.Logger log4JLogger = ((Log4JLogger)log).getLogger();
			log4JLogger.setLevel(level);
		}
	}
	
}
