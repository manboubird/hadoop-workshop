package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountSimpleMapperTest extends TestCase {

	private WordCountSimpleMapper mapper;
	private MapDriver<LongWritable, Text, Text, LongWritable> mapperDriver;

	@Before
	public void setUp() {
		mapper = new WordCountSimpleMapper();
		mapperDriver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
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
}
