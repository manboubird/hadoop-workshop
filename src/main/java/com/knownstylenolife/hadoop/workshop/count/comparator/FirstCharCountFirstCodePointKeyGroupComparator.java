package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstCharCountFirstCodePointKeyGroupComparator extends WritableComparator {

	protected FirstCharCountFirstCodePointKeyGroupComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		// for logging
		System.err.println(">>> FirstCharCountFirstCodePointKeyGroupComparator#compare() is called!!!. w1.firstCharCodePoint = [ " + ((Text)w1).charAt(0) + " ], w2.firstCharCodePoint = [ " + ((Text)w2).charAt(0) +" ]");
		return Integer.valueOf(((Text)w1).charAt(0)).compareTo(((Text)w2).charAt(0));
	}
}
