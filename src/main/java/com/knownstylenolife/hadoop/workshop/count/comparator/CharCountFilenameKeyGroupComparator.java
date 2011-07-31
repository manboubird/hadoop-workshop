package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountFilenameKeyGroupComparator extends WritableComparator {

	protected CharCountFilenameKeyGroupComparator() {
		super(CharCountMapOutputKeyWritable.class, true);
	}

	public int compare(CharCountMapOutputKeyWritable w1, CharCountMapOutputKeyWritable w2) {
		return w1.get().filename.compareTo(w2.get().filename);
	}
}
