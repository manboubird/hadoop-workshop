package com.knownstylenolife.hadoop.workshop.count.partitioner;


import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.knownstylenolife.hadoop.workshop.count.partitioner.WordCountKeyPrefixPartitioner;

public class WordCountKeyPrefixPartitionerTest {

	private WordCountKeyPrefixPartitioner partitioner;
	private Text inputKey;
	private LongWritable inputValue;
	private int numPartitions;
	
	@Before
	public void setUp() throws Exception {
		partitioner = new WordCountKeyPrefixPartitioner();
		inputKey = new Text();
		inputValue  = new LongWritable();
		numPartitions = WordCountKeyPrefixPartitioner.NUM_REDUCE_TASKS;
	}

	@Test
	public void testGetPartitionKEYVALUEInt() {
		ImmutableMap<String, PartitionNumber> data 
			= new ImmutableMap.Builder<String, PartitionNumber>()
				.put("a", PartitionNumber.FORMAR_ALPHABET)
				.put("abc", PartitionNumber.FORMAR_ALPHABET)
				.put("ABC", PartitionNumber.FORMAR_ALPHABET)
				.put("mno", PartitionNumber.FORMAR_ALPHABET)
				.put("MNO", PartitionNumber.FORMAR_ALPHABET)
				.put("nop", PartitionNumber.LATTER_ALPHABET)
				.put("NOP", PartitionNumber.LATTER_ALPHABET)
				.put("z01", PartitionNumber.LATTER_ALPHABET)
				.put("Z01", PartitionNumber.LATTER_ALPHABET)
				.put("s", PartitionNumber.LATTER_ALPHABET)
				.put("012", PartitionNumber.NUMBER)
				.put("9AB", PartitionNumber.NUMBER)
				.build();
		for(Entry<String, PartitionNumber> entry : data.entrySet()) {
			inputKey.set(entry.getKey());
			int actual = partitioner.getPartition(inputKey, inputValue, numPartitions);
			assertEquals("Illegal partition number. input key = " + inputKey.toString(), 
				entry.getValue().value(), actual);
		}
	}

	private static enum PartitionNumber {
		FORMAR_ALPHABET(0),
		NUMBER(1),
		LATTER_ALPHABET(2);
		
		int value;
		private PartitionNumber(int value) {
			this.value = value;
		}
		private int value() {
			return value;
		}
	}
}
