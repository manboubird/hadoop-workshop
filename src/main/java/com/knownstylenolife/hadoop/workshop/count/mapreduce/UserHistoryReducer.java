package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

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
    public void reduce(UserHitoryMapOutputKeyWritable userHistoryKeys, Iterable<NullWritable> nullValues, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("START REDUCE");
//    		LOG.debug("[ inputKey = " + key.get().toString() + " ]");
    	}

    	LinkedList<UserHistoryData> continuousUrlIdUserHistoryList = new LinkedList<UserHistoryData>();

    	Iterator<NullWritable> nullIterator = nullValues.iterator();
    	while(nullIterator.hasNext()) {
    		nullIterator.next();
    		UserHistoryData uhd = userHistoryKeys.get();
    		if(LOG.isDebugEnabled()) {
    	    	LOG.debug("  New key = " + uhd.toString());
    	    }
    		if(uhd.cvId > 0) {
    			if(continuousUrlIdUserHistoryList.size() == 0) {
    				System.err.println("No urlId is in userHistoryList!");
    				return;
    			}

    			// cv
    			UserHistoryData lastUrlUhd = continuousUrlIdUserHistoryList.getLast();
				outputValue.set(String.format(
					"%s\t%s\t%s\t%d\t%d", 
					lastUrlUhd.datetime, uhd.datetime, uhd.userId, lastUrlUhd.urlId, uhd.cvId));
	    		context.write(NullWritable.get(), outputValue);
     		}else if(uhd.urlId > 0) {
    			if(continuousUrlIdUserHistoryList.size() > 0) {
    				// output assist history for current userHistoryData
					for(UserHistoryData assistFromUhd : continuousUrlIdUserHistoryList) {
						outputValue.set(String.format(
							"%s\t%s\t%s\t-\t-\tAssist [ %d -> %d ]", 
							assistFromUhd.datetime, uhd.datetime, uhd.userId, assistFromUhd.urlId, uhd.urlId));
			    		context.write(NullWritable.get(), outputValue);
					}
    			}
    			continuousUrlIdUserHistoryList.add(uhd);
    		}else{
				System.err.println("Illegal userHistoryData. " + uhd.toString());
    		}
    	}
    }
}