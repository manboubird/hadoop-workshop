package com.knownstylenolife.hadoop.workshop.count.tool;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceLocalTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class CharCountWithoutReducerToolMainTest extends MapReduceLocalTestCaseBase {

	private static Log LOG = LogFactory.getLog(CharCountWithoutReducerToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.INFO.toString();

	private CharCountWithoutReducerToolMain tool;
	
	private String inputDir = "CharCountWithoutReducerToolMainTest/testRun_input";
	private URL inputURL;
	
	private String expectedOutputDirPath = "CharCountWithoutReducerToolMainTest/testRun_expected";
	private List<URL> expectedOutputFileUrlList;
	
	@Before
	public void setUp() throws Exception {
		inputURL = Resources.getResource(getClass(), inputDir);
		expectedOutputFileUrlList = Lists.newLinkedList();
		for(File file: new File(Resources.getResource(getClass(), expectedOutputDirPath).toURI()).listFiles()) {
			expectedOutputFileUrlList.add(file.toURI().toURL());
		}
		Configuration conf = getConfiguration();
		HdfsUtil.setConfiguration(conf);
		tool = new CharCountWithoutReducerToolMain();
		tool.setConf(conf);
	}

	@Test
	public void testRun() throws Exception {
		prepareJob(new File(inputURL.toURI()).listFiles());
		assertThat( 
			ToolRunner.run(
				tool, 
				new String[] { 
					getInputDir().toString(), 
					getOutputDir().toString(), 
					MR_LOG_LEVEL
		}), is(0));

		Path[] actualOutputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		for(Path p: actualOutputFiles) {
			LOG.info("OUTPUT:\n" + DfsTestUtil.readOutputsToString(p, getConfiguration()));
		}
		assertOutputFiles(actualOutputFiles, expectedOutputFileUrlList.toArray(new URL[0]));
	}
}
