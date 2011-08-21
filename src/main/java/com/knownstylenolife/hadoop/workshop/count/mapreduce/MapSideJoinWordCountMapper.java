package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.knownstylenolife.hadoop.workshop.common.service.LinkDataService;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.MapSideJoinMapOutputValueWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.MapSideJoinWordData;

public class MapSideJoinWordCountMapper extends Mapper<LongWritable, Text, Text, MapSideJoinMapOutputValueWritable> {
	
	private Log LOG = LogFactory.getLog(MapSideJoinWordCountMapper.class);

	public static final String WORDS_REGEX = "([\\w-]+)([^\\w-]|$)";

	private Text outputKey;
	private MapSideJoinMapOutputValueWritable outputValue;
	private LinkDataService linkDataService;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new Text();
		outputValue = new MapSideJoinMapOutputValueWritable();
		
		linkDataService = new LinkDataService();
		try {
			File file = new File("LinkDb.zip");
			if(LOG.isDebugEnabled()) {
				LOG.debug("LinkDb.zip Path = " + file.getAbsolutePath());
				LOG.debug("LinkDb.zip dir contents : " + Joiner.on("\n").join(file.list()));
			}
			linkDataService.prepareDatabase(file.getAbsolutePath());
		} catch (Exception e) {
			System.err.println("Failed to prepareDatabase : " + e.toString());
		}
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
  
		if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \"" + value.toString() + "\"]");
    	}
    	
		Matcher matcher = Pattern.compile(WORDS_REGEX).matcher(value.toString());
		while(matcher.find()) {
			String word = matcher.group(1);
			String link = linkDataService.getMstData(word);
			if(link == null) {
				if(LOG.isDebugEnabled()) {
		    		LOG.debug("  Any link exists for word \"" + word + "\"");
		    	}
				continue;
			}
			outputKey.set(word);
			outputValue.set(new MapSideJoinWordData(1, link));
			
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  [ outputKey = \"" + outputKey.toString() + "\" ]" + "[ outputValue = \"" + outputValue.get().toString() + "\" ]");
        	}
			context.write(outputKey, outputValue);
		}

	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		try {
        	if(LOG.isDebugEnabled()) {
        		LOG.debug("  cleanup : shutDownDatabase");
        	}
        	linkDataService.shutDownDatabase();
		} catch (Exception e) {
			System.err.println("Failed to shutDownDatabase : " + e.toString());
		}
	}
}