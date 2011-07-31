package com.knownstylenolife.hadoop.workshop.count.mapreduce;


import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class GenericsCountSumReducerTest {

	private GenericsCountSumReducer<CharCountMapOutputKeyWritable> reducer;
	private ReduceDriver<CharCountMapOutputKeyWritable, LongWritable, CharCountMapOutputKeyWritable, LongWritable> reducerDriver;

	@Before
	public void setUp() {
		reducer = new GenericsCountSumReducer<CharCountMapOutputKeyWritable>();
		reducerDriver = new ReduceDriver<CharCountMapOutputKeyWritable, LongWritable, CharCountMapOutputKeyWritable, LongWritable>(reducer);
	}
	
	@Test
	public void testWordCountReducer() {
		CharCountMapOutputKeyWritable key = new CharCountMapOutputKeyWritable(new CharCountData("somefile", 0L, Character.codePointAt(new char[] { 'T' }, 0)));
		reducerDriver
			.withInput(
					key, 
				Arrays.asList(
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1), 
					new LongWritable(1)))
			.withOutput(
				key,
				new LongWritable(4))
			.runTest();
	}
}
