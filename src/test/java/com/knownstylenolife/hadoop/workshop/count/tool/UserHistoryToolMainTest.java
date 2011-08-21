package com.knownstylenolife.hadoop.workshop.count.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceLocalTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class UserHistoryToolMainTest extends MapReduceLocalTestCaseBase {

	private Log LOG = LogFactory.getLog(UserHistoryToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private UserHistoryToolMain tool;
	private String inputFilename = "UserHistoryToolMainTest/testRun_input/input.txt";
	private String expectedOutputFilePath = "UserHistoryToolMainTest/testRun_expected/part-r-00000";

	@Before
	public void setUp() throws Exception {
		tool = new UserHistoryToolMain();
		tool.setConf(getConfiguration());
	}
	
	@Test
	public void testRun() throws Exception {
		prepareJob(new File(Resources.getResource(getClass(), inputFilename).toURI()));
		assertThat( 
			ToolRunner.run(
				tool, 
				new String[] { 
					getInputDir().toString(), 
					getOutputDir().toString(), 
					MR_LOG_LEVEL
		}), is(0));
		
		LOG.info("OUTPUT:\n" + DfsTestUtil.readOutputsToString(getOutputDir(), getConfiguration()));
		
		Path[] outputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		// for local mode assertion. redueceTaskNum is set to 1.
		assertOutputFile(outputFiles[0], Resources.getResource(getClass(), expectedOutputFilePath));
	}

}
