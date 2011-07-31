package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CharCountMapOutputKeyWritable implements WritableComparable<CharCountMapOutputKeyWritable> {

	private CharCountData charCountData;

	public CharCountMapOutputKeyWritable() {}
	
	public CharCountMapOutputKeyWritable(CharCountData charCountData) {
		set(charCountData);
	}
	
	public void set(CharCountData charCountData) {
		this.charCountData = charCountData;
	}
	
	public CharCountData get() {
		return charCountData;
	}

	public void readFields(DataInput in) throws IOException {
		CharCountData charCountData = new CharCountData();
		charCountData.filename = Text.readString(in);
		charCountData.offset = WritableUtils.readVLong(in);
		charCountData.codePoint = WritableUtils.readVInt(in);
		this.charCountData = charCountData;
	}

	public void write(DataOutput out) throws IOException {
		CharCountData mine = this.charCountData;
		Text.writeString(out, mine.filename);
		WritableUtils.writeVLong(out, mine.offset);
		WritableUtils.writeVInt(out, mine.codePoint);
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
		return Integer.valueOf(mine.codePoint).compareTo(another.codePoint);
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

	/**
	 * toString() is used to create reducerのoutput key String of TEXT.
	 * 
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return charCountData.filename + "\t" + charCountData.offset + "\t" + String.valueOf(Character.toChars(charCountData.codePoint));
	}

}
