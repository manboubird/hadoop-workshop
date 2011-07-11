package com.knownstylenolife.hadoop.workshop.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;

import com.knownstylenolife.hadoop.workshop.common.consts.ConfigurationConst;


public class HadoopLoggerUtil {

	private static Log LOG = LogFactory.getLog(HadoopLoggerUtil.class);

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
	
	public static void setLastArgAsLogLevel(String[] args, Configuration conf) {
		if(args.length > 0) {
			String lastArg = args[args.length - 1];
			if(lastArg.matches("(ALL|DEBUG|INFO|WARN|ERROR|FATAL|OFF|TRACE)")) {
				LOG.info("Set log level to " + lastArg);
				conf.set(ConfigurationConst.LOG_LEVEL, lastArg);
			}
			else {
				LOG.info("Last arg is not log level. skip. last arg = " + lastArg);
			}
		}
	}
}
