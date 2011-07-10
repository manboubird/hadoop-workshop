package com.knownstylenolife.hadoop.workshop.wordcount;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.wordcount.WordCountToolMain;

public class WordCountToolMainTest {

	private final String inputLocalDir = "src/main/resources/com/knownstylenolife/hadoopstudy/count/WordCountToolMain";
	private final String inputFilename = "hadoopwiki.txt";
	
	private final String inputDir = "target/input";
	
	private final String outputDir = "target/output";

	@Before
	public void setUp() throws Exception {
		
		HdfsUtil.deleteRecursively(outputDir);
		HdfsUtil.mkdirs(inputDir);
		HdfsUtil.copyFromLocalFile(new File(inputLocalDir, inputFilename).getAbsolutePath(), inputDir + "/" + inputFilename);

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMain() throws Exception {
		WordCountToolMain.main(new String[]{ inputDir, outputDir });
	}

}
