package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ReduceSideJoinMapOutputKeyWritable implements WritableComparable<ReduceSideJoinMapOutputKeyWritable> {

	private ReduceSideJoinWordData reduceSideJoinWordData;

	public ReduceSideJoinMapOutputKeyWritable() {}
	
	public ReduceSideJoinMapOutputKeyWritable(ReduceSideJoinWordData reduceSideJoinWordData) {
		set(reduceSideJoinWordData);
	}
	
	public void set(ReduceSideJoinWordData reduceSideJoinWordData) {
		this.reduceSideJoinWordData = reduceSideJoinWordData;
	}
	
	public ReduceSideJoinWordData get() {
		return reduceSideJoinWordData;
	}

	public void readFields(DataInput in) throws IOException {
		ReduceSideJoinWordData reduceSideJoinWordData = new ReduceSideJoinWordData();
		reduceSideJoinWordData.dataType = WritableUtils.readVInt(in);
		reduceSideJoinWordData.word = Text.readString(in);
		this.reduceSideJoinWordData = reduceSideJoinWordData;
	}

	public void write(DataOutput out) throws IOException {
		ReduceSideJoinWordData mine = this.reduceSideJoinWordData;
		WritableUtils.writeVInt(out, mine.dataType);
		Text.writeString(out, mine.word);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((reduceSideJoinWordData == null) ? 0
						: reduceSideJoinWordData.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReduceSideJoinMapOutputKeyWritable other = (ReduceSideJoinMapOutputKeyWritable) obj;
		if (reduceSideJoinWordData == null) {
			if (other.reduceSideJoinWordData != null)
				return false;
		} else if (!reduceSideJoinWordData.equals(other.reduceSideJoinWordData))
			return false;
		return true;
	}

	public int compareTo(ReduceSideJoinMapOutputKeyWritable logCountMapOutputKeyWritable) {
		ReduceSideJoinWordData mine = this.reduceSideJoinWordData;
		ReduceSideJoinWordData another = logCountMapOutputKeyWritable.reduceSideJoinWordData;
		int cmp = mine.word.compareTo(another.word);
		if (cmp != 0 ) {
			return cmp;
		}
		return Integer.valueOf(mine.dataType).compareTo(another.dataType); 
	}

	@Override
	public String toString() {
		return reduceSideJoinWordData.word;
	}
}
