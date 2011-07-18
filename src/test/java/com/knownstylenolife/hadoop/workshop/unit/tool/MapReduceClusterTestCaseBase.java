package com.knownstylenolife.hadoop.workshop.unit.tool;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;


public class MapReduceClusterTestCaseBase extends ClusterMapReduceTestCase {

	@SuppressWarnings("unused")
	private Log LOG = LogFactory.getLog(MapReduceClusterTestCaseBase.class.getName());

	private static final String HADOOP_ROOT = "target/hdc";
	private static final String TEST_BUILD_DATA = HADOOP_ROOT + "/build";
	private static final String HADOOP_LOG_DIR = HADOOP_ROOT + "/logs";
	private static final String HADOOP_TMP_DIR = HADOOP_ROOT + "/tmp/hadoop-username";
//	private static final String HADOOP_DFS_NAMENODE_LOGGING_LEVEL = "info";
//	private static final String HADOOP_ROOT_LOGGER = "WARN,console";

	protected void setUp() throws Exception {
		new File(HADOOP_LOG_DIR).mkdirs();
		new File(HADOOP_TMP_DIR).mkdirs();
		setClusterEnvs();
		Properties prop = new Properties();
		prop.put("hadoop.tmp.dir", HADOOP_TMP_DIR);
//		prop.put("dfs.namenode.logging.level", HADOOP_DFS_NAMENODE_LOGGING_LEVEL);
	    startCluster(true, prop);
	}
	
	protected void prepareJob(File... inputFiles) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.uploadLocalFileToInputDir(getFileSystem(), getInputDir(), inputFiles);
	}

	protected void prepareJob(String... inputs) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.createInputFiles(getInputDir(), getFileSystem(), inputs);
	}
	
	private void setClusterEnvs() {
		System.setProperty("test.build.data", TEST_BUILD_DATA);
		System.setProperty("hadoop.log.dir", HADOOP_LOG_DIR);
//		System.setProperty("hadoop.root.logger", HADOOP_ROOT_LOGGER);
		System.setProperty("javax.xml.parsers.SAXParserFactory",
				"com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
	}
}
