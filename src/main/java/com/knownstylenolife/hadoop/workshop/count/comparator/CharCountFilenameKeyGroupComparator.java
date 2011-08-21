package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountFilenameKeyGroupComparator extends WritableComparator {

	protected CharCountFilenameKeyGroupComparator() {
		super(CharCountMapOutputKeyWritable.class, true);
	}

	@Override
	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		// for logging
//		 System.err.println(">>> CharCountFilenameKeyGroupComparator#compare() is called!!!. w1 = [ " + w1.toString() + " ], w2 = [ " + w2.toString() +" ]");
		return ((CharCountMapOutputKeyWritable)w1).get().filename.compareTo(((CharCountMapOutputKeyWritable)w2).get().filename);
	}
}
