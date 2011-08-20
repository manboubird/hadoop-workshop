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

public class ReduceSideJoinToolMainTest extends MapReduceLocalTestCaseBase {

	private Log LOG = LogFactory.getLog(ReduceSideJoinToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private ReduceSideJoinToolMain tool;
	private String mstDataInputDirPath = "ReduceSideJoinToolMainTest/testRun_input/mstData";
	private File mstDataInputDir;
	private String wordCountDataInputDirPath = "ReduceSideJoinToolMainTest/testRun_input/wordCountData";
	private File wordCountDataInputDir;
	private String expectedOutputFilePath = "ReduceSideJoinToolMainTest/testRun_expected/part-r-00000";

	@Before
	public void setUp() throws Exception {
		mstDataInputDir = new File(Resources.getResource(getClass(), mstDataInputDirPath).toURI());
		wordCountDataInputDir = new File(Resources.getResource(getClass(), wordCountDataInputDirPath).toURI());
		tool = new ReduceSideJoinToolMain();
		tool.setConf(getConfiguration());
	}
	
	@Test
	public void testRun() throws Exception {
		prepareJobWithDirs(
			mstDataInputDir,
			wordCountDataInputDir);
		assertThat( 
			ToolRunner.run(
				tool, 
				new String[] { 
					new Path(getInputDir(), wordCountDataInputDir.getName()).toString(), 
					new Path(getInputDir(), mstDataInputDir.getName()).toString(), 
					getOutputDir().toString(), 
					MR_LOG_LEVEL
		}), is(0));
		
		LOG.info("OUTPUT:\n" + DfsTestUtil.readOutputsToString(getOutputDir(), getConfiguration()));
		
		Path[] outputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		// for local mode assertion. redueceTaskNum is set to 1.
		assertOutputFile(outputFiles[0], Resources.getResource(getClass(), expectedOutputFilePath));
	}

}
