package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountSimpleReducerTest extends TestCase {

	private WordCountSimpleReducer reducer;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reducerDriver;

	@Before
	public void setUp() {
		reducer = new WordCountSimpleReducer();
		reducerDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(reducer);
	}
	
	@Test
	public void testWordCountReducer() {
		reducerDriver
			.withInput(
				new Text("This"), 
				Arrays.asList(
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1)))
			.withOutput(
				new Text("This"), 
				new LongWritable(4))
			.runTest();
	}
}
