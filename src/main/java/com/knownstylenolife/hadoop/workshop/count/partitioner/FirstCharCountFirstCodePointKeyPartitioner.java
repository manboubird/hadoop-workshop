package com.knownstylenolife.hadoop.workshop.count.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstCharCountFirstCodePointKeyPartitioner extends Partitioner<Text, LongWritable>{
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		System.err.println(">>> FirstCharCountFirstCodePointKeyPartitioner#getPartition() is called!!!. firstCharCodePoint = " + key.charAt(0) + ", numPartitions = " + numPartitions + ", partitionId = " + key.charAt(0) % numPartitions);
	    // unicode scalar value is always positive number.
		return  key.charAt(0) % numPartitions;
	}
}
