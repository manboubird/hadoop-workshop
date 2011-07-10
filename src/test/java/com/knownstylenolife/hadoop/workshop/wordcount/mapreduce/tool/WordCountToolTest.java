package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool.WordCountTool.WordCountMapper;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool.WordCountTool.WordCountReducer;

public class WordCountToolTest extends TestCase {
	
	private static Log LOG = LogFactory.getLog(WordCountToolTest.class);
	
	private WordCountMapper mapper;
	private MapDriver<LongWritable, Text, Text, LongWritable> mapperDriver;
	
	private WordCountReducer reducer;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reducerDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapreduceDriver;
	
	@Before
	public void setUp() {
		mapper = new WordCountMapper();
		mapperDriver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);

		reducer = new WordCountReducer();
		reducerDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(reducer);
	
		mapreduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>(mapper, reducer);
	}
	
	@Test
	public void testWordCountMapper() {
		mapperDriver.withInput(new LongWritable(0), new Text("This is a pen"))
			.withOutput(new Text("This"), new LongWritable(1))
			.withOutput(new Text("is"), new LongWritable(1))
			.withOutput(new Text("a"), new LongWritable(1))
			.withOutput(new Text("pen"), new LongWritable(1))
			.runTest();
	}
	
	@Test
	public void testWordCountReducer() {
		reducerDriver.withInput(new Text("This"), Arrays.asList(new LongWritable(1), new LongWritable(1), new LongWritable(1), new LongWritable(1)))
			.withOutput(new Text("This"), new LongWritable(4))
			.runTest();
	}
	
	private String inputFilePath = "src/test/resources/com/knownstylenolife/hadoop/workshop/wordcount/WordCountToolMain/hadoop-wikipedia.txt";
		
	@SuppressWarnings("unchecked")
	@Test
	public void testRun() throws IOException {
		String input = FileUtils.readFileToString(new File(inputFilePath), Charsets.UTF_8.name());
		mapreduceDriver.addInput(new LongWritable(0), new Text(input));
		
		List<Pair<Text, LongWritable>> outputList = Lists.newLinkedList();
		outputList.add(new Pair(new Text("1"), new LongWritable(1)));
		outputList.add(new Pair(new Text("2"), new LongWritable(1)));
		outputList.add(new Pair(new Text("3"), new LongWritable(1)));
		outputList.add(new Pair(new Text("4"), new LongWritable(1)));
		outputList.add(new Pair(new Text("5"), new LongWritable(1)));
		outputList.add(new Pair(new Text("6"), new LongWritable(1)));
		outputList.add(new Pair(new Text("7"), new LongWritable(1)));
		outputList.add(new Pair(new Text("Apache"), new LongWritable(2)));
		outputList.add(new Pair(new Text("Cutting"), new LongWritable(1)));
		outputList.add(new Pair(new Text("Doug"), new LongWritable(1)));
		outputList.add(new Pair(new Text("File"), new LongWritable(1)));
		// TODO add rest of the expected results.
	
		List<Pair<Text, LongWritable>> reduceOutputs = mapreduceDriver.run();
		
		int max = reduceOutputs.size();
	    for(int i = 0; i < max; i++) {
	    	Pair<Text, LongWritable> pair = reduceOutputs.get(i);
	    	LOG.debug("pair = " + pair.toString());
	    	assertReduceOutputKey(pair.getFirst());
	    	
	    	if(i < outputList.size()) {
		    	if(!pair.equals(outputList.get(i))) {
		    		fail("Invalid pair is found. pair = " + pair.toString());
		    	}
	    	}
	    }
	}
	
	private Pattern pattern = Pattern.compile("^\\w+$");

	private void assertReduceOutputKey(Text key) {
		Matcher matcher = pattern.matcher(key.toString());
		boolean isMatched = matcher.matches();
		assertTrue(isMatched);
	}
}
