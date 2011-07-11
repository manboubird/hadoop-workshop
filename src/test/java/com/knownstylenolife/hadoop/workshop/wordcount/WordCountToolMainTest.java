package com.knownstylenolife.hadoop.workshop.wordcount;

import java.io.File;
import java.net.URL;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;

public class WordCountToolMainTest {

	private final String inputDir = "target/input";
	private final String outputDir = "target/output";

	@Before
	public void setUp() throws Exception {
		String filename = "WordCountToolMainTest_hadoop-wikipedia.txt";
		URL url = getClass().getResource(filename);
		
		HdfsUtil.deleteFileIfExists(inputDir + "/" + filename);
		HdfsUtil.deleteRecursively(outputDir);
		HdfsUtil.mkdirs(inputDir);
		HdfsUtil.copyFromLocalFile(new File(url.getFile()).getAbsolutePath(), inputDir + "/" + filename);
	}

	@Test
	public void testMain() throws Exception {
		WordCountToolMain.main(new String[]{ inputDir, outputDir });
		final String DEL = "\t";
		String[] expectedOutputs = {
				"1" + DEL + "1", 
				"2" + DEL + "1", 
				"3" + DEL + "1", 
				"4" + DEL + "1", 
				"5" + DEL + "1", 
				"6" + DEL + "1", 
				"7" + DEL + "1", 
				"Apache" + DEL + "2", 
				"Cutting" + DEL + "1", 
				"Doug" + DEL + "1", 
				"File" + DEL + "1"
				// TODO add rest of the expected results.
				};
		Assert.assertArrayEquals(
			expectedOutputs, 
			Files.readLines(new File(outputDir, "part-r-00000"), Charsets.UTF_8).toArray(new String[0])
		);
	}

}
