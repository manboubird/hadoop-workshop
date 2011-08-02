package com.knownstylenolife.hadoop.workshop.etc;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.base.Strings;
import com.knownstylenolife.hadoop.workshop.count.comparator.CharCountFilenameKeyGroupComparator;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.CharCountMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.GenericsCountSumReducer;
import com.knownstylenolife.hadoop.workshop.count.partitioner.CharCountFilenamePartitioner;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceClusterTestCaseBase;

public class CompareToLogTest extends MapReduceClusterTestCaseBase {

	public void setUp() throws Exception {
	    initLogger();
		super.setUp();
	}

	private void initLogger() {
		Logger.getRootLogger().removeAllAppenders();
		
		Properties logProperties = new Properties();
		logProperties.put("log4j.rootLogger", "WARN,stdout");
		logProperties.put("log4j.threshhold", "ALL");
		logProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		logProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
		logProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%-5p %c{2} (%F:%M(%L)) - %m%n");

		logProperties.put("log4j.logger.org.apache.hadoop.mapred", "DEBUG");
		logProperties.put("log4j.logger.org.apache.hadoop.mapred.Counters", "FATAL");
		logProperties.put("log4j.logger.org.apache.hadoop.mapreduce.", "DEBUG");
		logProperties.put("log4j.logger.org.apache.hadoop.fs", "DEBUG");
		logProperties.put("log4j.logger.org.apache.hadoop.hdfs", "FATAL");
		logProperties.put("log4j.logger.org.apache.hadoop.util", "FATAL");
	
		logProperties.put("log4j.logger.com.knownstylenolife.hadoop", "DEBUG");

	    PropertyConfigurator.configure(logProperties);
	}

	public void tearDown() throws Exception {
		super.tearDown();
	}

	public void testRun() throws Exception {
		FSDataOutputStream out1 = getFileSystem().create(
				new Path(getInputDir(), "inputfile-1.txt"));
		out1.writeUTF("AB\nCD");
		out1.close();

		FSDataOutputStream out2 = getFileSystem().create(
				new Path(getInputDir(), "inputfile-2.txt"));
		out2.writeUTF("ああ\nいい");
		out2.close();

		Configuration conf = getFileSystem().getConf();
		Job job = new Job(conf, "LOG compareTo");
		job.setJarByClass(getClass());

		FileInputFormat.setInputPaths(job, getInputDir());
		FileOutputFormat.setOutputPath(job, getOutputDir());

		job.setMapperClass(CharCountMapper.class);
		job.setReducerClass(GenericsCountSumReducer.class);

		job.setMapOutputKeyClass(CharCountMapOutputKeyWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setPartitionerClass(CharCountFilenamePartitioner.class);
		job.setCombinerClass(GenericsCountSumReducer.class);

		job.setNumReduceTasks(5);

		job.setGroupingComparatorClass(CharCountFilenameKeyGroupComparator.class);

		System.err.println(">>> START: submit job!!! " + Strings.repeat("*", 150));
		job.waitForCompletion(true);
		System.err.println(">>> END: job is completed!!!" + Strings.repeat("*", 150));
	}
}
