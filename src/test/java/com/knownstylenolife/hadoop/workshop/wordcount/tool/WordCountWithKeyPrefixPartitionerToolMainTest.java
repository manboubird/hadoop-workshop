package com.knownstylenolife.hadoop.workshop.wordcount.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceClusterTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class WordCountWithKeyPrefixPartitionerToolMainTest extends MapReduceClusterTestCaseBase {

	private Log LOG = LogFactory.getLog(WordCountWithKeyPrefixPartitionerToolMainTest.class);
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private WordCountWithKeyPrefixPartitionerToolMain tool;
	private String inputFilename = "hadoop-wikipedia.txt";

	private String expectedOutputDirPath = "WordCountWithKeyPrefixPartitionerToolMain/testRun_expected";
	private List<File> expectedOutputFileURL;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		expectedOutputFileURL = Lists.newLinkedList();
		for(File file: new File(Resources.getResource(getClass(), expectedOutputDirPath).toURI()).listFiles()) {
			expectedOutputFileURL.add(file);
		}
		tool = new WordCountWithKeyPrefixPartitionerToolMain();
		tool.setConf(createJobConf());
		prepareJob(new File(Resources.getResource(getClass(), inputFilename).toURI()));
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testRun() throws Exception {
		assertEquals(0, 
			ToolRunner.run(
				tool, 
				new String[] { 
					getInputDir().toString(), 
					getOutputDir().toString(), 
					MR_LOG_LEVEL
		}));
		Path[] outputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		assertEquals(3, outputFiles.length);
		assertOutputFiles(outputFiles);
	}
	
	private void assertOutputFiles(Path[] outputFiles) throws IOException {
		for(int i=0; i< outputFiles.length; i++ ){
			Path path = outputFiles[i];
			BufferedReader br = new BufferedReader(new InputStreamReader(getFileSystem().open(path)));
			List<String> expectedLineList = Files.readLines(expectedOutputFileURL.get(i), Charsets.UTF_8);
			int expectedLineListSize = expectedLineList.size();
			String actualLine;
			for(int j = 0; j < expectedLineListSize; j++) {
				assertNotNull("Actual file is ended. file = " + path.toUri().toString(), 
					actualLine = br.readLine());
				assertEquals("Does not match line!! line = " + (j + 1) + ", expected = " + expectedLineList.get(j) + ", actual = " + actualLine, 
					expectedLineList.get(j), actualLine);
			}
			assertNull("actual file is no EOF", br.readLine());
			br.close();
		}
	}
}
