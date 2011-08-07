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
import com.knownstylenolife.hadoop.workshop.count.comparator.FirstCharCountFirstCodePointKeyGroupComparator;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.GenericsCountSumReducer;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.FirstCharCountSumReducer;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.WordCountLowerCaseMapper;
import com.knownstylenolife.hadoop.workshop.count.partitioner.FirstCharCountFirstCodePointKeyPartitioner;


public class FirstCharCountToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(FirstCharCountToolMain.class);
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		LogUtil.setLastArgAsLogLevel(args, conf);
		
		Job job = new Job(conf, "Prefix Char Count");
		job.setJarByClass(getClass());

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		LOG.info("input = " + HdfsUtil.makeQualifedPath(inputPath).toString() + 
				", output = " + HdfsUtil.makeQualifedPath(outputPath).toString());

		FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(WordCountLowerCaseMapper.class);
        job.setReducerClass(FirstCharCountSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(GenericsCountSumReducer.class);
        job.setPartitionerClass(FirstCharCountFirstCodePointKeyPartitioner.class);
        job.setGroupingComparatorClass(FirstCharCountFirstCodePointKeyGroupComparator.class);

        job.setNumReduceTasks(2);
        return job.waitForCompletion(true) ? 0 : 1;
	}

    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new FirstCharCountToolMain(), args));
    }
}
