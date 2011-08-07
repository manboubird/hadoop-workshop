package com.knownstylenolife.hadoop.workshop.unit.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
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
	
	protected Configuration getConfiguration() throws IOException {
		Configuration conf = getFileSystem().getConf();
		String s = createJobConf().get("mapred.job.tracker");
		if(s == null) {
			throw new IllegalStateException("Cannot set mapred.job.tracker");
		}
		// set MiniMRCluster's job tracker info, otherwise LocalJobRunner will be run.
		conf.set("mapred.job.tracker", s);
		return conf;
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
		
		// The absence of valid hadoop.log.dir system property results in 
		// a NPE or IOException being thrown by the test case during cluster setup. 
		System.setProperty("hadoop.log.dir", HADOOP_LOG_DIR);
		
//		System.setProperty("hadoop.root.logger", HADOOP_ROOT_LOGGER);
		System.setProperty("javax.xml.parsers.SAXParserFactory",
				"com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
	}
	
	protected void assertOutputFiles(Path[] actualFilePathes, URL[] expectedOutputFileUrls) throws IOException {
		assertThat("# of actual pathes not equals to # of expeted output files.", actualFilePathes.length, is(expectedOutputFileUrls.length));
		for(int i=0; i< actualFilePathes.length; i++ ){
			assertOutputFile(actualFilePathes[i], expectedOutputFileUrls[i]);
		}
	}
	
	protected void assertOutputFile(Path actualOutputFile, URL expectedFileUrl) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(getFileSystem().open(actualOutputFile)));

		List<String> expectedLineResultList = Resources.readLines(expectedFileUrl, Charsets.UTF_8);
		int expectedLineResultListSize = expectedLineResultList.size();
		String actualLine;
		for(int i = 0; i < expectedLineResultListSize; i++) {
			assertThat("Actual file is ended", 
				actualLine = br.readLine(), not(nullValue()));
			assertThat("Does not match line!! line = " + (i + 1) + ", expected = " + expectedLineResultList.get(i) + ", actual = " + actualLine, 
				actualLine, is(expectedLineResultList.get(i)));
		}
		assertThat("actual file is not ended yet!! ", 
			br.readLine(), is(nullValue()));
		br.close();
	}
}
