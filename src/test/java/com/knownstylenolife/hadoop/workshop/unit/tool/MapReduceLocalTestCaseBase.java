package com.knownstylenolife.hadoop.workshop.unit.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;


public class MapReduceLocalTestCaseBase {

	@SuppressWarnings("unused")
	private Log LOG = LogFactory.getLog(MapReduceLocalTestCaseBase.class
			.getName());

	private static final String HADOOP_ROOT = "target/hd";
	private static final String HADOOP_TMP_DIR = HADOOP_ROOT + "/tmp/hadoop-username";
	private static final String IN_DIR = HADOOP_ROOT + "/input";
	private static final String OUT_DIR = HADOOP_ROOT + "/output";

	private static Configuration conf;
	private static FileSystem localFs;
	static {
		conf = new Configuration();
		// explicitly set local file system and local job runner.
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		conf.set("hadoop.tmp.dir", HADOOP_TMP_DIR);
		new File(HADOOP_TMP_DIR).mkdirs();
		try {
			localFs = FileSystem.getLocal(conf);
		} catch (IOException io) {
			throw new RuntimeException("problem getting local fs", io);
		}
	}

	protected Configuration getConfiguration() {
		return conf;
	}
	
	protected FileSystem getFileSystem() {
		return localFs;
	}

	protected Path getInputDir() {
		return new Path(IN_DIR);
	}

	protected Path getOutputDir() {
		return new Path(OUT_DIR);
	}

	protected void prepareJobWithDirs(File... inputDirs) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		for(File dir: inputDirs) {
			Preconditions.checkState(dir.isDirectory(), "It's not directory. " + dir.getAbsolutePath());
			DfsTestUtil.uploadLocalFileToInputDir(
				getFileSystem(), 
				new Path(getInputDir(), dir.getName()), 
				dir.listFiles());
		}
	}
	
	protected void prepareJob(File... inputFiles) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.uploadLocalFileToInputDir(getFileSystem(), getInputDir(), inputFiles);
	}

	protected void prepareJob(String... inputs) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.createInputFiles(getInputDir(), getFileSystem(), inputs);
	}

	protected void assertOutputLines(List<String> actualLineList, URL expectedFileUrl) throws IOException {
		List<String> expectedLineList = Resources.readLines(expectedFileUrl, Charsets.UTF_8);
		assertThat(actualLineList.size(), is(expectedLineList.size()));
		int expectedLineListSize = expectedLineList.size();
	    for(int i = 0; i < expectedLineListSize; i++) {
			assertThat("Does not match line!! line = " + (i + 1) + ", expected = " + expectedLineList.get(i) + ", actual = " + actualLineList.get(i), 
				actualLineList.get(i), is(expectedLineList.get(i)));
		}
	}
	
	protected void assertOutputFiles(Path[] actualFilePathes, URL[] expectedOutputFileUrls) throws IOException {
		assertThat("# of actual pathes not equals to # of expeted output files.", actualFilePathes.length, is(expectedOutputFileUrls.length));
		for(int i=0; i< actualFilePathes.length; i++ ){
			assertOutputFile(actualFilePathes[i], expectedOutputFileUrls[i]);
		}
	}
	
	protected void assertOutputFile(Path actualOutputFile, URL expectedFileUrl) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(getFileSystem().open(actualOutputFile)));

		List<String> expectedLineResultList = Resources.readLines(expectedFileUrl, Charsets.UTF_8);
		int expectedLineResultListSize = expectedLineResultList.size();
		String actualLine;
		for(int i = 0; i < expectedLineResultListSize; i++) {
			assertThat("Actual file is ended", actualLine = br.readLine(), not(nullValue()));
			assertThat("Does not match line!! line = " + (i + 1) + ", expected = " + expectedLineResultList.get(i) + ", actual = " + actualLine, 
				actualLine, is(expectedLineResultList.get(i)));
		}
		assertThat("actual file is not ended yet!! ", br.readLine(), is(nullValue()));
		br.close();
	}
}
