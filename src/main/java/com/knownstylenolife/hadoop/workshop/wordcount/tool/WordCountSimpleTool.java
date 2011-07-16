package com.knownstylenolife.hadoop.workshop.wordcount.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.knownstylenolife.hadoop.workshop.common.util.HadoopLoggerUtil;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.WordCountSimpleMapper;
import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.WordCountSimpleReducer;


public class WordCountSimpleTool extends Configured implements Tool {

	Log LOG = LogFactory.getLog(WordCountSimpleTool.class);
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		HadoopLoggerUtil.setLastArgAsLogLevel(args, conf);
		
		Job job = new Job(conf, "Word Count Simple");
		job.setJarByClass(getClass());
		
		LOG.info("input = " + args[0] + ", output = " + args[1]);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountSimpleMapper.class);
        job.setReducerClass(WordCountSimpleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
	}
	
    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountSimpleTool(), args));
    }
}
