package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;

public class PerSplitSemiJoinWordKeyGroupComparator extends WritableComparator {

	protected PerSplitSemiJoinWordKeyGroupComparator() {
		super(PerSplitSemiJoinMapOutputKeyWritable.class, true);
	}

	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		return ((PerSplitSemiJoinMapOutputKeyWritable)w1).word.compareTo(((PerSplitSemiJoinMapOutputKeyWritable)w2).word);
	}
}
