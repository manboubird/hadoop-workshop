package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountSumReducerTest {

	private WordCountSumReducer reducer;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reducerDriver;

	@Before
	public void setUp() {
		reducer = new WordCountSumReducer();
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
