package com.knownstylenolife.hadoop.workshop.count.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceClusterTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class WordCountWithKeyPrefixPartitionerToolMainTest extends MapReduceClusterTestCaseBase {

	@SuppressWarnings("unused")
	private Log LOG = LogFactory.getLog(WordCountWithKeyPrefixPartitionerToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private WordCountWithKeyPrefixPartitionerToolMain tool;
	private String inputFilename = "WordCountWithKeyPrefixPartitionerToolMain/testRun_input/hadoop-wikipedia.txt";

	private String expectedOutputDirPath = "WordCountWithKeyPrefixPartitionerToolMain/testRun_expected";
	private List<URL> expectedOutputFileUrlList;

	@BeforeClass
	public void setUp() throws Exception {
		super.setUp();
		expectedOutputFileUrlList = Lists.newLinkedList();
		for(File file: new File(Resources.getResource(getClass(), expectedOutputDirPath).toURI()).listFiles()) {
			expectedOutputFileUrlList.add(file.toURI().toURL());
		}
		tool = new WordCountWithKeyPrefixPartitionerToolMain();
		tool.setConf(createJobConf());
		prepareJob(new File(Resources.getResource(getClass(), inputFilename).toURI()));
	}

	@AfterClass
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testRun() throws Exception {
		assertThat( 
			ToolRunner.run(
				tool, 
				new String[] { 
					getInputDir().toString(), 
					getOutputDir().toString(), 
					MR_LOG_LEVEL
		}), is(0));
		Path[] outputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		assertOutputFiles(outputFiles, expectedOutputFileUrlList.toArray(new URL[0]));
	}

}
