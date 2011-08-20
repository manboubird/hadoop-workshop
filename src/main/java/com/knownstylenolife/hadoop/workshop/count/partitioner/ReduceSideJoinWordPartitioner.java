package com.knownstylenolife.hadoop.workshop.count.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;

public class ReduceSideJoinWordPartitioner extends Partitioner<ReduceSideJoinMapOutputKeyWritable, Text>{
	@Override
	public int getPartition(ReduceSideJoinMapOutputKeyWritable key, Text value, int numPartitions) {
		return  (key.get().word.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
