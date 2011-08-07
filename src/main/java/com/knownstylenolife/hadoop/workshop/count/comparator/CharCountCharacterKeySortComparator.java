package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.CharCountMapOutputKeyWritable;

public class CharCountCharacterKeySortComparator extends WritableComparator{

	protected CharCountCharacterKeySortComparator() {
		super(CharCountMapOutputKeyWritable.class, true);
	}

	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		// for logging
//		System.err.println(">>> CharCountCharacterKeySortComparator#compare() is called!!!. w1 = [ " + w1.toString() + " ], w2 = [ " + w2.toString() +" ]");
		return String.valueOf(Character.toChars(((CharCountMapOutputKeyWritable)w1).get().codePoint))
			.compareTo(String.valueOf(Character.toChars(((CharCountMapOutputKeyWritable)w2).get().codePoint)));
	}
}
