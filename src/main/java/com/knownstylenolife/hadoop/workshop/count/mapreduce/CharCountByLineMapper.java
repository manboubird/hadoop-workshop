package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;

public class CharCountByLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private Log LOG = LogFactory.getLog(CharCountByLineMapper.class);

	private Text outputKey;
	private LongWritable outputValue;
	private String filename;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new Text();
		outputValue = new LongWritable(0);
		
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
		
		// key = [ character's codePoint ]
		// value = [ # of characater's codePoint appear in line ]
		Map<Integer, Long> codePointCountMap = new TreeMap<Integer, Long>();

		char[] chars = value.toString().toCharArray();
		int len = value.toString().length();
		
		for(int i = 0, codePoint = 0; i < len; i += Character.charCount(codePoint)) {
			codePoint = Character.codePointAt(chars, i);
			if(!Character.isWhitespace(Character.toChars(codePoint)[0])) {
				Long count = codePointCountMap.get(codePoint);
				codePointCountMap.put(codePoint, (count == null ? 1L : ++count));
			}
		}
		
		Counter lineCounter = context.getCounter("Total line counters", filename);
		lineCounter.increment(1L);
		long lineNumber = lineCounter.getValue();
		
		for(Entry<Integer, Long> entry: codePointCountMap.entrySet()) {
			outputKey.set(filename + "\t" + lineNumber + "\t" + String.valueOf(Character.toChars(entry.getKey())));
			outputValue.set(entry.getValue().longValue());
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]" + "[ outputValue = \"" + outputValue.toString() + "\" ]");
        	}
			context.write(outputKey, outputValue);
		}
	}
}