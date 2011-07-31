package com.knownstylenolife.hadoop.workshop.count.writable;


public class CharCountData {

	public String filename;
	public Long offset;
	public int codePoint;
	
	public CharCountData() {}
	
	public CharCountData(String filename, Long offset, int codePoint) {
		this.offset = offset;
		this.codePoint = codePoint;
		this.filename = filename;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + codePoint;
		result = prime * result
				+ ((filename == null) ? 0 : filename.hashCode());
		result = prime * result + ((offset == null) ? 0 : offset.hashCode());
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
		CharCountData other = (CharCountData) obj;
		if (codePoint != other.codePoint)
			return false;
		if (filename == null) {
			if (other.filename != null)
				return false;
		} else if (!filename.equals(other.filename))
			return false;
		if (offset == null) {
			if (other.offset != null)
				return false;
		} else if (!offset.equals(other.offset))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CharCountData [codePoint=" + codePoint + ", filename=" + filename
				+ ", offset=" + offset + "]";
	}

}
