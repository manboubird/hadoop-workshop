package com.knownstylenolife.hadoop.workshop.unit.tool;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

	protected void prepareJob(File... inputFiles) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.uploadLocalFileToInputDir(getFileSystem(), getInputDir(), inputFiles);
	}

	protected void prepareJob(String... inputs) throws IOException {
		DfsTestUtil.cleanDirs(getInputDir(), getOutputDir(), getFileSystem());
		DfsTestUtil.createInputFiles(getInputDir(), getFileSystem(), inputs);
	}
}
