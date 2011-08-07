package com.knownstylenolife.hadoop.workshop.count.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountFilenamePartitioner extends Partitioner<CharCountMapOutputKeyWritable, LongWritable>{

	public int getPartition(CharCountMapOutputKeyWritable key, LongWritable value, int numPartitions) {
	    int partitionId = (key.get().filename.hashCode() & Integer.MAX_VALUE) % numPartitions;
//		System.err.println(">>> CharCountFilenamePartitioner#getPartition() is called!!!. filename = " + key.get().filename + ", hashCode = " + key.get().filename.hashCode() + ", partitionId = " + partitionId + ", numPartitions = " + numPartitions);
	    return partitionId;
	}
}
