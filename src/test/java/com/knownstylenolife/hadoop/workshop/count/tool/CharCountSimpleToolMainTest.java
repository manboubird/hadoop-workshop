package com.knownstylenolife.hadoop.workshop.count.tool;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceLocalTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class CharCountSimpleToolMainTest extends MapReduceLocalTestCaseBase {

	@SuppressWarnings("unused")
	private static Log LOG = LogFactory.getLog(CharCountSimpleToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private CharCountSimpleToolMain tool;
	
	private String inputDir = "CharCountSimpleToolMainTest/testRun_input";
	private URL inputURL;
	
	private String expectedOutputFilePath = "CharCountSimpleToolMainTest/testRun_expected/part-r-00000";
	private URL expectedOutputFileURL;
	
	@Before
	public void setUp() throws Exception {
		tool = new CharCountSimpleToolMain();
		tool.setConf(getConfiguration());
		inputURL = Resources.getResource(getClass(), inputDir);
		expectedOutputFileURL = Resources.getResource(getClass(), expectedOutputFilePath);
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
		assertThat(actualOutputFiles.length, is(1));
		assertOutputFile(actualOutputFiles[0], expectedOutputFileURL);
	}
}
