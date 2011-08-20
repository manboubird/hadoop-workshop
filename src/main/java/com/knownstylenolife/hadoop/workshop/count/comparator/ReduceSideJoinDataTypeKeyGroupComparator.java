package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinMapOutputKeyWritable;
import com.knownstylenolife.hadoop.workshop.count.writable.ReduceSideJoinWordData;

public class ReduceSideJoinDataTypeKeyGroupComparator extends WritableComparator {

	protected ReduceSideJoinDataTypeKeyGroupComparator() {
		super(ReduceSideJoinMapOutputKeyWritable.class, true);
	}

	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		ReduceSideJoinWordData d1 = ((ReduceSideJoinMapOutputKeyWritable)w1).get();
		ReduceSideJoinWordData d2 = ((ReduceSideJoinMapOutputKeyWritable)w2).get();
		return d1.word.compareTo(d2.word);
	}
}
