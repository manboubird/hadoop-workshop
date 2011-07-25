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
		mapperDriver.withInput(new LongWritable(0), new Text("This is a pen"))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('T'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('h'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('i'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('s'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('i'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('s'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('a'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('p'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('e'))), new LongWritable(1))
			.withOutput(new CharCountMapOutputKeyWritable(new CharCountData(filename, offset, new Character('n'))), new LongWritable(1))
			.runTest();
	}	
}
