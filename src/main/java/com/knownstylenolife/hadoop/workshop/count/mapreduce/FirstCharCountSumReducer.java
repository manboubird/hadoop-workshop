package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;

public class FirstCharCountSumReducer extends Reducer<Text, LongWritable, Text, Text> {

	Log LOG = LogFactory.getLog(FirstCharCountSumReducer.class);

	private Text outputKey;
	private Text outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new Text();
		outputValue = new Text();
	}
	
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ inputKey = " + key.toString() + " ]");
    	}
    	
    	long uniqueCount = 0L;
    	String lastWord = "";
    	long totalCount = 0L;
    	for(LongWritable value : values) {
    		totalCount += value.get();
    		String word = key.toString();
    		if(!lastWord.equals(word)) {
    			uniqueCount++;
    	    	if(LOG.isDebugEnabled()) {
    	    		LOG.debug("  New word = \"" + word  + "\", unique/total = \"" + uniqueCount+ "\t" + totalCount + "\"");
    	    	}
    			lastWord = word;
    		}
    	}
    	
    	outputKey.set(key.toString().substring(0, 1));
    	outputValue.set(uniqueCount + "\t" + totalCount);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\", outputValue = \"" + outputValue.toString() + "\"]");
    	}
    	
    	context.write(outputKey, outputValue);
    }
}