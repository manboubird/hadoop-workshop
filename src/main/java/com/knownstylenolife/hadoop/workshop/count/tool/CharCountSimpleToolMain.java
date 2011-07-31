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

import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.comparator.CharCountFilenameKeyGroupComparator;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.CharCountMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.GenericsCountSumReducer;
import com.knownstylenolife.hadoop.workshop.count.partitioner.CharCountFilenamePartitioner;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;


public class CharCountSimpleToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(CharCountSimpleToolMain.class);
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		LogUtil.setLastArgAsLogLevel(args, conf);
		
		Job job = new Job(conf, "Char Count Simple");
		job.setJarByClass(getClass());

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		LOG.info("input = " + HdfsUtil.makeQualifedPath(inputPath).toString() + 
				", output = " + HdfsUtil.makeQualifedPath(outputPath).toString());

		FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(CharCountMapper.class);
        job.setReducerClass(GenericsCountSumReducer.class);

        job.setMapOutputKeyClass(CharCountMapOutputKeyWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setPartitionerClass(CharCountFilenamePartitioner.class);
        job.setCombinerClass(GenericsCountSumReducer.class);
        
        int inputFileNum = HdfsUtil.getPathes(inputPath).length;
        if(inputFileNum == 0) {
        	LOG.error("There is no input files.");
        	return 1;
        }
        int numReduceTasks = Double.valueOf(Math.ceil(Double.valueOf(inputFileNum) / 2)).intValue();
        LOG.info("set Reduce Tasks Num to " + numReduceTasks);
        job.setNumReduceTasks(numReduceTasks);
        
        job.setGroupingComparatorClass(CharCountFilenameKeyGroupComparator.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
	}
	
    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new CharCountSimpleToolMain(), args));
    }
}
