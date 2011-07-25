package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountMapper extends Mapper<LongWritable, Text, CharCountMapOutputKeyWritable, LongWritable> {
	
	private Log LOG = LogFactory.getLog(CharCountMapper.class);

	private CharCountMapOutputKeyWritable outputKey;
	private LongWritable outputValue;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new CharCountMapOutputKeyWritable();
		outputValue = new LongWritable(1);
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" +
    				  "[ value = \"" + value.toString() + "\"]");
    	}

		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		long offset = key.get();
		for(char ch : value.toString().toCharArray()) {
			
			if(!Character.isWhitespace(ch)) {
				CharCountData charCountData = new CharCountData(filename.toString(), offset, ch);
				outputKey.set(charCountData);
				
	        	if(LOG.isDebugEnabled()) {
	        		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]" +
	        					"[ outputValue = \"" + outputValue.toString() + "\" ]");
	        	}
				context.write(outputKey, outputValue);
			}
		}
	}
}