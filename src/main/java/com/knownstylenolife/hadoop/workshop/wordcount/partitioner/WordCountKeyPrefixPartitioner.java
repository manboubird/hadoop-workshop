package com.knownstylenolife.hadoop.workshop.wordcount.partitioner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountKeyPrefixPartitioner extends Partitioner<Text, LongWritable>{

	private Matcher formerAlphabetMatcher = Pattern.compile("^[a-mA-M].*").matcher("");
	private Matcher numberMatcher = Pattern.compile("^[0-9].*").matcher("");

	public static final int NUM_REDUCE_TASKS = 3;
	
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		String str = key.toString();
		int i = formerAlphabetMatcher.reset(str).matches() ? 0 
				: numberMatcher.reset(str).matches() ? 1 : 2;
	    return  (i & Integer .MAX_VALUE) % numPartitions;
	}
}
