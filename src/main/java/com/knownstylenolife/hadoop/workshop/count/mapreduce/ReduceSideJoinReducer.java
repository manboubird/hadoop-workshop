package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinWordData;

public class ReduceSideJoinReducer extends Reducer<ReduceSideJoinMapOutputKeyWritable, Text, ReduceSideJoinMapOutputKeyWritable, Text> {

	Log LOG = LogFactory.getLog(ReduceSideJoinReducer.class);

	private Text outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new Text();
	}
	
    @Override
    public void reduce(ReduceSideJoinMapOutputKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = " + key.get().toString() + " ]");
    	}
    	ReduceSideJoinWordData reduceSideJoinWordData = key.get();
    	Iterator<Text> it = values.iterator();
    	String link = it.next().toString();
    	if("".equals(link)) {
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  No link exists for word \"" + reduceSideJoinWordData.word + "\"");
        	}
        	return;
    	}
    	long count = 0L;
    	while(it.hasNext()) {
    		it.next();
    		count++;
    	}
    	outputValue.set(link + "\t" + count);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputKey = \"" + key.toString() + "\" ][ outputValue = \"" + outputValue.toString() + "\"]");
    	}
    	
    	context.write(key, outputValue);
    }
}