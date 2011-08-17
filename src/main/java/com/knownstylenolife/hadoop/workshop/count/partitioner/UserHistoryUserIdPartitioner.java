package com.knownstylenolife.hadoop.workshop.count.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.knownstylenolife.hadoop.workshop.count.writable.UserHitoryMapOutputKeyWritable;

public class UserHistoryUserIdPartitioner extends Partitioner<UserHitoryMapOutputKeyWritable, NullWritable>{
	@Override
	public int getPartition(UserHitoryMapOutputKeyWritable key, NullWritable value, int numPartitions) {
		return  (key.get().userId.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
