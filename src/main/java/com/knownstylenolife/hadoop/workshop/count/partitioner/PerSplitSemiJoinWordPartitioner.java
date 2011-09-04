package com.knownstylenolife.hadoop.workshop.count.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;

public class PerSplitSemiJoinWordPartitioner extends Partitioner<PerSplitSemiJoinMapOutputKeyWritable, LongWritable>{
	@Override
	public int getPartition(PerSplitSemiJoinMapOutputKeyWritable key, LongWritable value, int numPartitions) {
		return  (key.word.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
