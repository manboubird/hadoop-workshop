package com.knownstylenolife.hadoop.workshop.count.partitioner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountKeyPrefixPartitioner extends Partitioner<Text, LongWritable>{

	private Pattern formerAlphabetPattern = Pattern.compile("^[a-mA-M].*");
	private Pattern numberPattern = Pattern.compile("^[0-9].*");

	// must be set NUM_REDUCE_TASKS to job.setNumReduceTasks().
	public static final int NUM_REDUCE_TASKS = 3;
	
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		String str = key.toString();
		Matcher formerAlphabetMatcher = formerAlphabetPattern.matcher(str);
		Matcher numberMatcher = numberPattern.matcher(str);
		int i = formerAlphabetMatcher.matches() ? 0 
				: numberMatcher.matches() ? 1 : 2;
	    return  (i & Integer.MAX_VALUE) % numPartitions;
	}
}
