package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinWordData;

public class ReduceSideJoinWordCountMapper extends Mapper<LongWritable, Text, ReduceSideJoinMapOutputKeyWritable, Text> {
	
	private Log LOG = LogFactory.getLog(ReduceSideJoinWordCountMapper.class);

	public static final String WORDS_REGEX = "([\\w-]+)([^\\w-]|$)";

	private ReduceSideJoinMapOutputKeyWritable outputKey;
	private Text outputValue;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new ReduceSideJoinMapOutputKeyWritable();
		outputValue = new Text("");
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \"" + value.toString() + "\"]");
    	}
    	
		Matcher matcher = Pattern.compile(WORDS_REGEX).matcher(value.toString());
		while(matcher.find()) {
			
			ReduceSideJoinWordData reduceSideJoinWordData = new ReduceSideJoinWordData();
			reduceSideJoinWordData.word = matcher.group(1);
			reduceSideJoinWordData.dataType = ReduceSideJoinWordData.DataType.WORD_COUNT_DATA;
			outputKey.set(reduceSideJoinWordData);
			
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  [ outputKey = \"" + outputKey.get().toString() + "\" ]" + "[ outputValue = \"" + outputValue.toString() + "\" ]");
        	}
			context.write(outputKey, outputValue);
		}
	}
}