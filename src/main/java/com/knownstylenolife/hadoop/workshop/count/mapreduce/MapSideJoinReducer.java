package com.knownstylenolife.hadoop.workshop.count.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.writable.MapSideJoinMapOutputValueWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.MapSideJoinWordData;

public class MapSideJoinReducer extends Reducer<Text, MapSideJoinMapOutputValueWritable, Text, MapSideJoinMapOutputValueWritable> {

	Log LOG = LogFactory.getLog(MapSideJoinReducer.class);

	private MapSideJoinMapOutputValueWritable outputValue;

	@Override
	protected void setup(Context context) {
		LogUtil.setLogLevel(LOG, context.getConfiguration());
		outputValue = new MapSideJoinMapOutputValueWritable();
	}
	
    @Override
    public void reduce(Text key, Iterable<MapSideJoinMapOutputValueWritable> values, Context context) throws IOException, InterruptedException {

    	if(LOG.isDebugEnabled()) {
    		LOG.debug("[ key = " + key.toString() + " ]");
    	}
    	Iterator<MapSideJoinMapOutputValueWritable> iterator = values.iterator();
		MapSideJoinWordData outputMapSideJoinWordData = iterator.next().get();
    	while(iterator.hasNext()) {
    		outputMapSideJoinWordData.count += iterator.next().get().count;
    	}
    	outputValue.set(outputMapSideJoinWordData);
    	
    	if(LOG.isDebugEnabled()) {
    		LOG.debug("  [ outputKey = \"" + key.toString() + "\" ][ outputValue = \"" + outputValue.get().toString() + "\"]");
    	}
    	context.write(key, outputValue);
    }
}