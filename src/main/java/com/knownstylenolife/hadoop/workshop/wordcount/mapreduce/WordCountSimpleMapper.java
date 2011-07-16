package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.knownstylenolife.hadoop.workshop.common.util.HadoopLoggerUtil;

public class WordCountSimpleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private Log LOG = LogFactory.getLog(WordCountSimpleMapper.class);

	public static final String WORDS_REGEX = "(\\w+)([^\\w]|$)";
	private Matcher matcher;

	private Text outputKey;
	private LongWritable outputValue;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		HadoopLoggerUtil.setLogLevel(LOG, context.getConfiguration());
		
		matcher = Pattern.compile(WORDS_REGEX).matcher("");
		outputKey = new Text();
		outputValue = new LongWritable(1);
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" +
    				  "[ value = \"" + value.toString() + "\"]");
    	}
    	
		matcher.reset(value.toString());
		while(matcher.find()) {
			
			outputKey.set(matcher.group(1));
			
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]" +
        					"[ outputValue = \"" + outputValue.toString() + "\" ]");
        	}
			context.write(outputKey, outputValue);
		}
	}
}