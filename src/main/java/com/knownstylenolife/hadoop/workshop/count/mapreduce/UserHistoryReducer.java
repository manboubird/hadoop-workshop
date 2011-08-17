package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.UserHistoryData;
import com.knownstylenolife.hadoop.workshop.count.writable.UserHitoryMapOutputKeyWritable;

public class UserHistoryReducer extends Reducer<UserHitoryMapOutputKeyWritable, NullWritable, NullWritable, Text> {

	Log LOG = LogFactory.getLog(UserHistoryReducer.class);

	private Text outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new Text();
	}
	
    @Override
    public void reduce(UserHitoryMapOutputKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("START REDUCE");
//    		LOG.debug("[ inputKey = " + key.get().toString() + " ]");
    	}
    	Queue<UserHistoryData> queue = new LinkedList<UserHistoryData>();
    
    	Iterator<NullWritable> itr = values.iterator();
    	while(itr.hasNext()) {
    		itr.next();
    		UserHistoryData uhd = key.get();
    		if(LOG.isDebugEnabled()) {
    	    	LOG.debug("  New key = " + uhd.toString());
    	    }
    		if(uhd.cvId > 0) {
    			queue.add(uhd);
    		}else if(uhd.urlId > 0) {
    			UserHistoryData cvUhd = queue.poll();
    			if(cvUhd != null) {
    				outputValue.set(uhd.datetime + "\t" + cvUhd.datetime + "\t" + uhd.userId + "\t" + uhd.urlId + "\t" + cvUhd.cvId);
    				if(LOG.isDebugEnabled()) {
    					LOG.debug("  [outputValue = \"" + outputValue.toString() + "\"]");
    				}
    	    		context.write(NullWritable.get(), outputValue);
    			}else{
    				System.err.println("Illegal poll.");
    			}
    		}else{
				System.err.println("Illegal uhd. " + uhd.toString());
    		}
    	}
    }
}