package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;

public class WordCountSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	Log LOG = LogFactory.getLog(WordCountSumReducer.class);

	private LongWritable outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new LongWritable();
	}
	
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = " + key.toString() + " ]");
    	}
    	
    	long count = 0L;
    	for(LongWritable value : values) {
    		count += value.get();
    	}
    	outputValue.set(count);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputValue = \"" + outputValue.get() + "\"]");
    	}
    	
    	context.write(key, outputValue);
    }
}