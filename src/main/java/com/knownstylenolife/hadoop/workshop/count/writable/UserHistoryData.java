package com.knownstylenolife.hadoop.workshop.count.writable;

public class UserHistoryData {

	public String datetime;
	public String userId;
	public long urlId;
	public long cvId;
	
	public UserHistoryData() {}
	
	public UserHistoryData(String datetime, String userId, long urlId, long cvId) {
		this.datetime = datetime;
		this.userId = userId;
		this.urlId = urlId;
		this.cvId = cvId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (cvId ^ (cvId >>> 32));
		result = prime * result
				+ ((datetime == null) ? 0 : datetime.hashCode());
		result = prime * result + (int) (urlId ^ (urlId >>> 32));
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
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
		UserHistoryData other = (UserHistoryData) obj;
		if (cvId != other.cvId)
			return false;
		if (datetime == null) {
			if (other.datetime != null)
				return false;
		} else if (!datetime.equals(other.datetime))
			return false;
		if (urlId != other.urlId)
			return false;
		if (userId == null) {
			if (other.userId != null)
				return false;
		} else if (!userId.equals(other.userId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "UserHistoryData [userId=" + userId + ", datetime=" + datetime
				+ ",  urlId=" + urlId + ", cvId=" + cvId + "]";
	}
}
