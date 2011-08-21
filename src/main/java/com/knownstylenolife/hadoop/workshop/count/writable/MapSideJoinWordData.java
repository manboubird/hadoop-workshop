package com.knownstylenolife.hadoop.workshop.count.writable;

public class MapSideJoinWordData {

	public long count;
	public String link;
	
	public MapSideJoinWordData() {}
	
	public MapSideJoinWordData(long count, String link) {
		this.count = count;
		this.link = link;
	}

	@Override
	public String toString() {
		return "MapSideJoinWordData [link=" + link + ", count=" + count + "]";
	}
}
