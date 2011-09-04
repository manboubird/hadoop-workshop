package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;

public class PerSplitSemiJoinReducer extends Reducer<PerSplitSemiJoinMapOutputKeyWritable, LongWritable, PerSplitSemiJoinMapOutputKeyWritable, Text> {

	Log LOG = LogFactory.getLog(PerSplitSemiJoinReducer.class);

	private Text outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new Text();
	}
	
    @Override
    public void reduce(PerSplitSemiJoinMapOutputKeyWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    	if(LOG.isDebugEnabled()) { LOG.debug("  [ outputKey = [ word=" + key.word + " ][ link=" + key.link + " ]]"); }
    	
    	String word = key.word;
    	String link = key.link;
    	
    	if(link == null) {
        	System.err.println("  No link exists!!");
        	return;
    	}
    	
    	long count = 0L;
    	for(LongWritable longWritable :  values) {
    		count += longWritable.get();
    	}
    	if(count == 0) {
    		if(LOG.isDebugEnabled()) { LOG.debug("  Zero count word \"" + word + "\""); }
    		return;
    	}
    	outputValue.set(link + "\t" + count);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputKey = \"" + key.toString() + "\" ][ outputValue = \"" + outputValue.toString() + "\"]");
    	}
    	if(LOG.isDebugEnabled()) { LOG.debug("  [ outputKey = [ word=" + key.word + " ][ link=" + key.link + " ]]" + "[ outputValue = \"" + outputValue.toString() + "\" ]"); }
    	context.write(key, outputValue);
    }
}