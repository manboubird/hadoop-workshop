package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PerSplitSemiJoinMapOutputKeyWritable implements WritableComparable<PerSplitSemiJoinMapOutputKeyWritable> {
	
	public String word;
	public String link;

	public PerSplitSemiJoinMapOutputKeyWritable() {}
	
	public void readFields(DataInput in) throws IOException {
		this.word = Text.readString(in);
		if(in.readBoolean()) {
			link = Text.readString(in);
		} else {
			link = null;
		}
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.word);
        if(link != null) {
            out.writeBoolean(true);
            Text.writeString(out, link);
        } else {
            out.writeBoolean(false);
        }
	}

	public int compareTo(PerSplitSemiJoinMapOutputKeyWritable another) {
		int cmp = this.word.compareTo(another.word);
		if (cmp != 0 ) {
			return cmp;
		}
		if(this.link == null) {
			cmp = another.link == null 
				? 0
				: 1; 		
		}else{
			cmp = another.link != null 
				? this.link.compareTo(another.link)
				: -1; 
		}
		return cmp;
	}

	@Override
	public String toString() {
		return this.word;
	}
}
