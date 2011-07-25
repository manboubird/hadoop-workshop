package com.knownstylenolife.hadoop.workshop.count.tool;

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

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.CharCountMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.CharCountSumCombiner;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.CharCountSumReducer;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;


public class CharCountSimpleToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(CharCountSimpleToolMain.class);
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		LogUtil.setLastArgAsLogLevel(args, conf);
		
		Job job = new Job(conf, "Char Count Simple");
		job.setJarByClass(getClass());
		
		LOG.info("input = " + args[0] + ", output = " + args[1]);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CharCountMapper.class);
        job.setReducerClass(CharCountSumReducer.class);

        job.setMapOutputKeyClass(CharCountMapOutputKeyWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setCombinerClass(CharCountSumCombiner.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
	}
	
    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new CharCountSimpleToolMain(), args));
    }
}
