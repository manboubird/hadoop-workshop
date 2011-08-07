package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountData;
import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountMapper extends Mapper<LongWritable, Text, CharCountMapOutputKeyWritable, LongWritable> {
	
	private Log LOG = LogFactory.getLog(CharCountMapper.class);

	private CharCountMapOutputKeyWritable outputKey;
	private LongWritable outputValue;
	private String filename;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new CharCountMapOutputKeyWritable();
		outputValue = new LongWritable(1);
		
		InputSplit inputSplit = context.getInputSplit();
		if(inputSplit instanceof FileSplit) {
			filename = ((FileSplit) inputSplit).getPath().getName();;
		}else {
			LOG.error("InputSplit is not FileSplit!!");
			filename = "UNKNOWN_FILENAME";
		}
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \"" + value.toString() + "\"]");
    	}
		
		long offset = key.get();

		char[] chars = value.toString().toCharArray();
		int len = value.toString().length();
		
		for(int i = 0, codePoint = 0; i < len; i += Character.charCount(codePoint)) {
			codePoint = Character.codePointAt(chars, i);
			if(!Character.isWhitespace(Character.toChars(codePoint)[0])) {
				CharCountData charCountData = new CharCountData(filename.toString(), offset, codePoint);
				outputKey.set(charCountData);
				
	        	if(LOG.isDebugEnabled()) {
	        		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]" + "[ outputValue = \"" + outputValue.toString() + "\" ]");
	        	}
				context.write(outputKey, outputValue);
			}
		}
	}
}