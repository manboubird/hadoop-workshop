package com.knownstylenolife.hadoop.workshop.wordcount;

import org.apache.hadoop.util.ToolRunner;

import com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool.WordCountTool;

public class WordCountToolMain {
    public static void main( String[] args) throws Exception
    {
		System.exit(ToolRunner.run(new WordCountTool(), new String[]{ args[1], args[2] }));
    }
}
