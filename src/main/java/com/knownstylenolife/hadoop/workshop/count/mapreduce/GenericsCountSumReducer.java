package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;

public class GenericsCountSumReducer<KEY> extends Reducer<KEY, LongWritable, KEY, LongWritable> {

	private Log LOG = LogFactory.getLog(GenericsCountSumReducer.class);

	private LongWritable outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new LongWritable();
	}

    @Override
    public void reduce(KEY key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = " + key.toString() + " ]");
    	}
    	
    	long count = 0L;
    	for(LongWritable value : values) {
    		count += value.get();
//    		System.err.println(">>> GenericsCountSumReducer#reduce: Iteration count = " + count);
    	}
    	outputValue.set(count);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputValue = \"" + outputValue.get() + "\"]");
    	}
    	
    	context.write(key, outputValue);
    }
}