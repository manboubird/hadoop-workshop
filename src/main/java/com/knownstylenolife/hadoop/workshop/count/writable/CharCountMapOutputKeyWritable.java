package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CharCountMapOutputKeyWritable implements WritableComparable<CharCountMapOutputKeyWritable>{

	private CharCountData charCountData;

	public CharCountMapOutputKeyWritable() {}
	
	public CharCountMapOutputKeyWritable(CharCountData logCountData) {
		set(logCountData);
	}
	
	public void set(CharCountData logCountData) {
		this.charCountData = logCountData;
	}
	
	public CharCountData get() {
		return charCountData;
	}

	public void readFields(DataInput in) throws IOException {
		CharCountData charCountData = new CharCountData();
		charCountData.filename = WritableUtils.readString(in);
		charCountData.offset = WritableUtils.readVLong(in);
		charCountData.character = WritableUtils.readString(in).charAt(0);
		this.charCountData = charCountData;
	}

	public void write(DataOutput out) throws IOException {
		CharCountData mine = this.charCountData;
		WritableUtils.writeString(out, mine.filename);
		WritableUtils.writeVLong(out, mine.offset);
		WritableUtils.writeString(out, mine.character.toString());
	}

	public int compareTo(CharCountMapOutputKeyWritable logCountMapOutputKeyWritable) {
		CharCountData mine = this.charCountData;
		CharCountData another = logCountMapOutputKeyWritable.charCountData;
		int cmp = mine.filename.compareTo(another.filename); 
		if (cmp != 0 ) {
			return cmp;
		}
		cmp = mine.offset.compareTo(another.offset); 
		if (cmp != 0 ) {
			return cmp;
		}
		return mine.character.compareTo(another.character);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CharCountMapOutputKeyWritable other = (CharCountMapOutputKeyWritable) obj;
		if (charCountData == null) {
			if (other.charCountData != null)
				return false;
		} else if (!charCountData.equals(other.charCountData))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CharCountMapOutputKeyWritable [charCountData=" + charCountData
				+ "]";
	}

}
