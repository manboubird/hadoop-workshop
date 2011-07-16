package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.WordCountSimpleMapper;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.WordCountSimpleReducer;
import com.knownstylenolife.hadoop.workshop.wordcount.tool.WordCountSimpleTool;

public class WordCountSimpleToolTest extends TestCase {
	
	private static Log LOG = LogFactory.getLog(WordCountSimpleToolTest.class);
	
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapreduceDriver;
	
	private String inputFilename = "WordCountToolTest_hadoop-wikipedia.txt";
	private String inputFilePath;
	
	private LinkedList<Pair<Text, LongWritable>>expectedResultList;
	
	private final String inputDirHdfs = "target/input";
	private final String outputDirHdfs = "target/output";

	private Matcher matcher;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		mapreduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>(new WordCountSimpleMapper(), new WordCountSimpleReducer());
		inputFilePath = getClass().getResource(inputFilename).getFile();
		matcher = Pattern.compile(WordCountSimpleMapper.WORDS_REGEX).matcher("");
		
		expectedResultList = Lists.newLinkedList();
		expectedResultList.add(new Pair(new Text("1"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("2"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("3"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("4"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("5"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("6"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("7"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("Apache"), new LongWritable(2)));
		expectedResultList.add(new Pair(new Text("Cutting"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("Doug"), new LongWritable(1)));
		expectedResultList.add(new Pair(new Text("File"), new LongWritable(1)));
		// TODO add rest of the expected results.
		Collections.unmodifiableList(expectedResultList);
	}
	
	/**
	 * Test mapreduce with MapReduceDriver.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapReduce() throws IOException {
		String inputValue = FileUtils.readFileToString(new File(inputFilePath), Charsets.UTF_8.name());
		mapreduceDriver.addInput(new LongWritable(0), new Text(inputValue));
		
		List<Pair<Text, LongWritable>> reduceOutputs = mapreduceDriver.run();
		int expectedLineResultListSize = expectedResultList.size();
	    for(int i = 0; i < expectedLineResultListSize; i++) {
	    	Pair<Text, LongWritable> actualPair = reduceOutputs.get(i);
	    	LOG.debug("pair = " + actualPair.toString());
	    	assertTrue("Invalid pair is found. pair = " + actualPair.toString(), 
	    				actualPair.equals(expectedResultList.get(i)));
	    	assertTrue("Invalid match key is found. pair = " + actualPair.toString(), 
	    				matcher.reset(actualPair.getFirst().toString()).matches());
	    }
	}
	
	private void setupTestRun() throws Exception {
		HdfsUtil.deleteFileIfExists(inputDirHdfs + "/" + inputFilename);
		HdfsUtil.deleteRecursivelyIfExists(outputDirHdfs);
		HdfsUtil.mkdirs(inputDirHdfs);
		HdfsUtil.copyFromLocalFile(new File(inputFilePath).getAbsolutePath(), inputDirHdfs + "/" + inputFilename);
	}

	/**
	 * test with standalone mode
	 * @throws Exception
	 */
	@Test
	public void testRun() throws Exception {
		setupTestRun();
		assertEquals(
			0, 
			ToolRunner.run(
				new WordCountSimpleTool(), 
				new String[] { 
					inputDirHdfs, 
					outputDirHdfs, 
					//"DEBUG"
		}));
		List<String> expectedLineResultList = Lists.transform(expectedResultList, new Function<Pair<Text, LongWritable>, String>() {
			public String apply(Pair<Text, LongWritable> input) {
				return new StringBuilder()
					.append(input.getFirst().toString())
					.append("\t")
					.append(input.getSecond().get()).toString();
			}			
		});
		List<String> actualResultList = Files.readLines(new File(outputDirHdfs, "part-r-00000"), Charsets.UTF_8);
		int expectedLineResultListSize = expectedLineResultList.size();
		for(int i = 0; i < expectedLineResultListSize; i++) {
			Assert.assertEquals("Does not match line!! line = " + (i + 1) + ", expected = " + expectedLineResultList.get(i) + ", actual = " + actualResultList.get(i), 
				expectedLineResultList.get(i), actualResultList.get(i));
		}
	}
}
