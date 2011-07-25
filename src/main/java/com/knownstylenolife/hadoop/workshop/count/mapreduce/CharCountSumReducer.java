package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountSumReducer extends Reducer<CharCountMapOutputKeyWritable, LongWritable, Text, LongWritable> {

	Log LOG = LogFactory.getLog(CharCountSumReducer.class);

	private Text outputKey;
	private LongWritable outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new Text();
		outputValue = new LongWritable();
	}
	
    @Override
    public void reduce(CharCountMapOutputKeyWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = " + key.toString() + " ]");
    	}
    	
    	CharCountData data = key.get();
    	
    	String s = data.filename + "\t" + data.offset + "\t" + data.character;
    	outputKey.set(s);
    	
    	long count = 0L;
    	for(LongWritable value : values) {
    		count += value.get();
    	}
    	outputValue.set(count);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputValue = \"" + outputValue.get() + "\"]");
    	}
    	
    	context.write(outputKey, outputValue);
    }
}