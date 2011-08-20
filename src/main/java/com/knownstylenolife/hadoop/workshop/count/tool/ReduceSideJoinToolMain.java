package com.knownstylenolife.hadoop.workshop.count.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.comparator.ReduceSideJoinDataTypeKeyGroupComparator;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.ReduceSideJoinMstDataMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.ReduceSideJoinReducer;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.ReduceSideJoinWordCountMapper;
import com.knownstylenolife.hadoop.workshop.count.partitioner.ReduceSideJoinWordPartitioner;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;


public class ReduceSideJoinToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(ReduceSideJoinToolMain.class);
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		LogUtil.setLastArgAsLogLevel(args, conf);
		
		Job job = new Job(conf, "Reduce-side Join");
		job.setJarByClass(getClass());

		Path wordCountDataInputPath = new Path(args[0]);
		Path mstDataInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		LOG.info("wordCountDataInputPath = " + HdfsUtil.makeQualifedPath(wordCountDataInputPath).toString()); 
		LOG.info("mstDataInputPath       = " + HdfsUtil.makeQualifedPath(mstDataInputPath).toString());
		LOG.info("outputPath             = " + HdfsUtil.makeQualifedPath(outputPath).toString());

		MultipleInputs.addInputPath(job, wordCountDataInputPath, TextInputFormat.class, ReduceSideJoinWordCountMapper.class);
		MultipleInputs.addInputPath(job, mstDataInputPath, TextInputFormat.class, ReduceSideJoinMstDataMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setMapOutputKeyClass(ReduceSideJoinMapOutputKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(ReduceSideJoinMapOutputKeyWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(ReduceSideJoinDataTypeKeyGroupComparator.class);
        job.setPartitionerClass(ReduceSideJoinWordPartitioner.class);
        
//        job.setNumReduceTasks(2);
        return job.waitForCompletion(true) ? 0 : 1;
	}

    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReduceSideJoinToolMain(), args));
    }
}
