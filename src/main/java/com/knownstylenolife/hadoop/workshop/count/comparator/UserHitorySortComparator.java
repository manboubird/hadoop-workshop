package com.knownstylenolife.hadoop.workshop.count.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.knownstylenolife.hadoop.workshop.count.writable.UserHistoryData;
import com.knownstylenolife.hadoop.workshop.count.writable.UserHitoryMapOutputKeyWritable;

public class UserHitorySortComparator extends WritableComparator{

	protected UserHitorySortComparator() {
		super(UserHitoryMapOutputKeyWritable.class, true);
	}

	@SuppressWarnings("unchecked")
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserHistoryData u1 = ((UserHitoryMapOutputKeyWritable)w1).get();
		UserHistoryData u2 = ((UserHitoryMapOutputKeyWritable)w2).get();
		int cmp = u1.userId.compareTo(u2.userId);
		if (cmp != 0 ) {
			return -cmp;
		}
		cmp = u1.datetime.compareTo(u2.datetime); 
		if (cmp != 0 ) {
			return -cmp;
		}
		cmp = Long.valueOf(u1.urlId).compareTo(u2.urlId);
		if (cmp != 0 ) {
			return -cmp;
		}
		return -Long.valueOf(u1.cvId).compareTo(u2.cvId);
	}
}
