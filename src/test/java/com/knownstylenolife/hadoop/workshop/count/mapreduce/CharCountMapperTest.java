package com.knownstylenolife.hadoop.workshop.count.mapreduce;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountMapperTest {

	private CharCountMapper mapper;
	private MapDriver<LongWritable, Text, CharCountMapOutputKeyWritable, LongWritable> mapperDriver;

	@Before
	public void setUp() {
		mapper = new CharCountMapper();
		mapperDriver = new MapDriver<LongWritable, Text, CharCountMapOutputKeyWritable, LongWritable>(mapper);
	}

	@Test
	public void testCharCountMapper() {
		String filename = "somefile";
		long offset = 0;
		mapperDriver
			.withInput(new LongWritable(0), new Text("This is a pen. \ud867\ude15")) // with surrogate-pairs character
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'T'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'h'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'i'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'s'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'i'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'s'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'a'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'p'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'e'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'n'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt(new char[] {'.'}, 0))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, Character.codePointAt("\ud867\ude15".toCharArray(), 0))), new LongWritable(1))
			.runTest();
	}	
}
