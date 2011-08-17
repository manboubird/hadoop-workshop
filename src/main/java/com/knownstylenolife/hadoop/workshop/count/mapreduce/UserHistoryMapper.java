package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.UserHistoryData;
import com.knownstylenolife.hadoop.workshop.count.writable.UserHitoryMapOutputKeyWritable;

public class UserHistoryMapper extends Mapper<LongWritable, Text, UserHitoryMapOutputKeyWritable, NullWritable> {
	
	private Log LOG = LogFactory.getLog(UserHistoryMapper.class);

	private UserHitoryMapOutputKeyWritable outputKey;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new UserHitoryMapOutputKeyWritable();
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \"" + value.toString() + "\"]");
    	}
			
		String[] splitted = value.toString().split("\t");
		if(splitted.length != 4) {
			System.err.println("Illegal line : " + key.toString());
			return;
		}
		outputKey.set(new UserHistoryData(splitted[0], splitted[1], Long.parseLong(splitted[2]), Long.parseLong(splitted[3])));
			
       	if(LOG.isDebugEnabled()) {
        	LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]");
        }
		context.write(outputKey, NullWritable.get());
	}
}