package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;

public class PerSplitSemiJoinMstDataMapper extends Mapper<LongWritable, Text, PerSplitSemiJoinMapOutputKeyWritable, LongWritable> {

	private Log LOG = LogFactory.getLog(PerSplitSemiJoinMstDataMapper.class);

	private PerSplitSemiJoinMapOutputKeyWritable outputKey;
	public static final LongWritable ZERO_OUTPUT_VALUE = new LongWritable(0L);

	private static final int SPLIT_LENGTH = 2;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new PerSplitSemiJoinMapOutputKeyWritable();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		if (LOG.isDebugEnabled()) { LOG.debug("[ key = \"" + key.get() + "\" ][ value = \"" + value.toString() + "\"]"); }

		String[] splitted = value.toString().split("\t");
		if (splitted.length != SPLIT_LENGTH) {
			System.err.println("Illegal line format. \"" + value.toString() + "\"");
			return;
		}
		outputKey.word = splitted[0];
		outputKey.link = splitted[1];
    	if(LOG.isDebugEnabled()) { LOG.debug("  [ outputKey = [ word=" + outputKey.word + " ][ link=" + outputKey.link + " ]]" + "[ outputValue = \"" + ZERO_OUTPUT_VALUE.get() + "\" ]"); }
		context.write(outputKey, ZERO_OUTPUT_VALUE);
	}
}