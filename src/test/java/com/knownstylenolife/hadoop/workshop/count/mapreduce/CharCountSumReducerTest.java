package com.knownstylenolife.hadoop.workshop.count.mapreduce;


import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountSumReducerTest {

	private CharCountSumReducer reducer;
	private ReduceDriver<CharCountMapOutputKeyWritable, LongWritable, Text, LongWritable> reducerDriver;

	@Before
	public void setUp() {
		reducer = new CharCountSumReducer();
		reducerDriver = new ReduceDriver<CharCountMapOutputKeyWritable, LongWritable, Text, LongWritable>(reducer);
	}
	
	@Test
	public void testWordCountReducer() {
		reducerDriver
			.withInput(
					new CharCountMapOutputKeyWritable(new CharCountData("somefile", 0L, new Character('T'))), 
				Arrays.asList(
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1)))
			.withOutput(
				new Text("somefile\t0\tT"), 
				new LongWritable(4))
			.runTest();
	}
}
