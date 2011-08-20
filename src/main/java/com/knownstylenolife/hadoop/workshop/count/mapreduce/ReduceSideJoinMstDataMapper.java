package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinWordData;

public class ReduceSideJoinMstDataMapper extends Mapper<LongWritable, Text, ReduceSideJoinMapOutputKeyWritable, Text> {

	private Log LOG = LogFactory.getLog(ReduceSideJoinMstDataMapper.class);

	private ReduceSideJoinMapOutputKeyWritable outputKey;
	private Text outputValue;

	private static final int SPLIT_LENGTH = 2;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputKey = new ReduceSideJoinMapOutputKeyWritable();
		outputValue = new Text();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws InterruptedException, IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("[ key = \"" + key.toString() + "\" ]" + "[ value = \""
					+ value.toString() + "\"]");
		}

		String[] splitted = value.toString().split("\t");
		if (splitted.length != SPLIT_LENGTH) {
			System.err.println("");
			return;
		}

		ReduceSideJoinWordData reduceSideJoinWordData = new ReduceSideJoinWordData();
		reduceSideJoinWordData.word = splitted[0];
		reduceSideJoinWordData.dataType = ReduceSideJoinWordData.DataType.MST_DATA;
		outputKey.set(reduceSideJoinWordData);
		outputValue.set(splitted[1]);

		if (LOG.isDebugEnabled()) {
			LOG.debug("  [ outputKey = \"" + outputKey.get().toString() + "\" ]" + 
					    "[ outputValue = \"" + outputValue.toString() + "\" ]");
		}
		context.write(outputKey, outputValue);
	}
}