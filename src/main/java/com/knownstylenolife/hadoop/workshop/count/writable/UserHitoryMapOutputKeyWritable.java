package com.knownstylenolife.hadoop.workshop.count.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class UserHitoryMapOutputKeyWritable implements WritableComparable<UserHitoryMapOutputKeyWritable> {

	private UserHistoryData userHistoryData;

	public UserHitoryMapOutputKeyWritable() {}
	
	public UserHitoryMapOutputKeyWritable(UserHistoryData userHistoryData) {
		set(userHistoryData);
	}
	
	public void set(UserHistoryData userHistoryData) {
		this.userHistoryData = userHistoryData;
	}
	
	public UserHistoryData get() {
		return userHistoryData;
	}

	public void readFields(DataInput in) throws IOException {
		UserHistoryData userHistoryData = new UserHistoryData();
		userHistoryData.datetime = Text.readString(in);
		userHistoryData.userId = Text.readString(in);
		userHistoryData.urlId = WritableUtils.readVLong(in);
		userHistoryData.cvId = WritableUtils.readVLong(in);
		this.userHistoryData = userHistoryData;
	}

	public void write(DataOutput out) throws IOException {
		UserHistoryData mine = this.userHistoryData;
		Text.writeString(out, mine.datetime);
		Text.writeString(out, mine.userId);
		WritableUtils.writeVLong(out, mine.urlId);
		WritableUtils.writeVLong(out, mine.cvId);
	}

	public int compareTo(UserHitoryMapOutputKeyWritable logCountMapOutputKeyWritable) {
		UserHistoryData mine = this.userHistoryData;
		UserHistoryData another = logCountMapOutputKeyWritable.userHistoryData;
		int cmp = mine.userId.compareTo(another.userId);
		if (cmp != 0 ) {
			return cmp;
		}
		cmp = mine.datetime.compareTo(another.datetime); 
		if (cmp != 0 ) {
			return cmp;
		}
		cmp = Long.valueOf(mine.urlId).compareTo(another.urlId);
		if (cmp != 0 ) {
			return cmp;
		}
		return Long.valueOf(mine.cvId).compareTo(another.cvId);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((userHistoryData == null) ? 0 : userHistoryData.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserHitoryMapOutputKeyWritable other = (UserHitoryMapOutputKeyWritable) obj;
		if (userHistoryData == null) {
			if (other.userHistoryData != null)
				return false;
		} else if (!userHistoryData.equals(other.userHistoryData))
			return false;
		return true;
	}	
}
