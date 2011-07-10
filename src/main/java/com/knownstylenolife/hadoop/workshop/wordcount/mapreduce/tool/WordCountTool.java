package com.knownstylenolife.hadoop.workshop.wordcount.mapreduce.tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.knownstylenolife.hadoop.workshop.common.consts.ConfigurationConst;
import com.knownstylenolife.hadoop.workshop.common.util.HadoopLoggerUtil;


public class WordCountTool extends Configured implements Tool {

	Log LOG = LogFactory.getLog(WordCountTool.class);

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		Log MAPLOG = LogFactory.getLog(WordCountMapper.class);

		private final String REGEX = "(\\w+)([^\\w]|$)";
		private Pattern pattern = Pattern.compile(REGEX);

		private final Text outputKeyText = new Text();
		private final LongWritable outputValueLongWritable = new LongWritable(1);
			
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			HadoopLoggerUtil.setLogLevel(MAPLOG, context.getConfiguration());
		}
		
		@Override
	    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
	  
			if(MAPLOG.isDebugEnabled()) {
	    		MAPLOG.debug("[ key = \"" + key.toString() + "\" ][ value = \"" + value.toString() + "\"]");
	    	}
	    	
			Matcher matcher = pattern.matcher(value.toString());
			while(matcher.find()) {
				
				outputKeyText.set(matcher.group(1));
				
	        	if(MAPLOG.isDebugEnabled()) {
	        		MAPLOG.debug("  [ outputKeyText = \"" + outputKeyText.toString() + "\" ]" +
	        					"[ outputValueLongWritable = \"" + outputValueLongWritable.toString() + "\" ]");
	        	}
				context.write(outputKeyText, outputValueLongWritable);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		Log REDUCELOG = LogFactory.getLog(WordCountReducer.class);

		private final LongWritable outputValueLongWritable = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			HadoopLoggerUtil.setLogLevel(REDUCELOG, context.getConfiguration());
		}
		
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
 
        	if(REDUCELOG.isDebugEnabled()) {
        		REDUCELOG.debug("[ key = " + key.toString() + " ]");
        	}
        	
        	long count = 0;
        	Iterator<LongWritable> it = values.iterator();
        	while(it.hasNext()) {
        		count += it.next().get();
        	}
        	outputValueLongWritable.set(count);
        	
        	if(REDUCELOG.isDebugEnabled()) {
        		REDUCELOG.debug("  [ outputValueLongWritable = \"" + outputValueLongWritable.get() + "\"]");
        	}
        	
        	context.write(key, outputValueLongWritable);
        }
    }
    
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		if(args.length >= 3) {
			String level = args[2];
			LOG.info("Set log level to " + level);
			conf.set(ConfigurationConst.LOG_LEVEL, level);
		}
		
		Job job = new Job(conf, "Word Count");
		job.setJarByClass(getClass());
		
		LOG.info(args[0] + ", " + args[1]);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

		return 0;
	}
}
