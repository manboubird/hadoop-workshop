package com.knownstylenolife.hadoop.workshop.count.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.WordCountMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.WordCountSumReducer;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceLocalTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;
import com.knownstylenolife.hadoop.workshop.unit.util.PairUtil;

public class WordCountSimpleMainToolTest extends MapReduceLocalTestCaseBase {
	
	@SuppressWarnings("unused")
	private static Log LOG = LogFactory.getLog(WordCountSimpleMainToolTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.WARN.toString();

	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapreduceDriver;
	private WordCountSimpleToolMain tool;
	
	private String inputFilename = "WordCountSimpleMainToolTest/testRun_input/hadoop-wikipedia.txt";
	private URL inputURL;
	
	private String expectedOutputFilePath = "WordCountSimpleMainToolTest/testRun_expected/part-r-00000";
	private URL expectedOutputFileURL;
	
	@Before
	public void setUp() throws Exception {
		mapreduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>(new WordCountMapper(), new WordCountSumReducer());
		tool = new WordCountSimpleToolMain();
		tool.setConf(getConfiguration());
		inputURL = Resources.getResource(getClass(), inputFilename);
		expectedOutputFileURL = Resources.getResource(getClass(), expectedOutputFilePath);
	}
	
	/**
	 * Test mapreduce with MapReduceDriver.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapReduce() throws IOException {
		mapreduceDriver.addInput(
			new LongWritable(0), 
			new Text(Resources.toString(inputURL, Charsets.UTF_8)));
		assertOutputLines(PairUtil.toStringList(mapreduceDriver.run()), expectedOutputFileURL);
	}
	
	/**
	 * test with standalone mode
	 * @throws Exception
	 */
	@Test
	public void testRun() throws Exception {
		prepareJob(new File(inputURL.toURI()));
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
