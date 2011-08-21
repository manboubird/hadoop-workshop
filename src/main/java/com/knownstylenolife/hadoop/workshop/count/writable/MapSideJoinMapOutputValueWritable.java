package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MapSideJoinMapOutputValueWritable implements Writable {

	private MapSideJoinWordData mapSideJoinWordData;

	public MapSideJoinMapOutputValueWritable() {}
	
	public MapSideJoinMapOutputValueWritable(MapSideJoinWordData mapSideJoinWordData) {
		set(mapSideJoinWordData);
	}
	
	public void set(MapSideJoinWordData mapSideJoinWordData) {
		this.mapSideJoinWordData = mapSideJoinWordData;
	}
	
	public MapSideJoinWordData get() {
		return mapSideJoinWordData;
	}

	public void readFields(DataInput in) throws IOException {
		MapSideJoinWordData mapSideJoinWordData = new MapSideJoinWordData();
		mapSideJoinWordData.count = WritableUtils.readVLong(in);
		mapSideJoinWordData.link = Text.readString(in);
		this.mapSideJoinWordData = mapSideJoinWordData;
	}

	public void write(DataOutput out) throws IOException {
		MapSideJoinWordData mine = this.mapSideJoinWordData;
		WritableUtils.writeVLong(out, mine.count);
		Text.writeString(out, mine.link);
	}

	@Override
	public String toString() {
		return mapSideJoinWordData.link + "\t" + mapSideJoinWordData.count;
	}
}
