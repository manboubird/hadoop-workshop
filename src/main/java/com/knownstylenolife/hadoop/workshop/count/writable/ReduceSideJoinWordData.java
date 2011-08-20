package com.knownstylenolife.hadoop.workshop.count.writable;

public class ReduceSideJoinWordData {

	public class DataType {
		public static final int WORD_COUNT_DATA = 1;
		public static final int MST_DATA = 0;
	}
	
	public int dataType;
	public String word;
	
	public ReduceSideJoinWordData() {}
	
	public ReduceSideJoinWordData(int dataType, String word) {
		this.dataType = dataType;
		this.word = word;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataType;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		ReduceSideJoinWordData other = (ReduceSideJoinWordData) obj;
		if (dataType != other.dataType)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ReduceSideJoinWordData [dataType=" + dataType + ", word="
				+ word + "]";
	}

}
