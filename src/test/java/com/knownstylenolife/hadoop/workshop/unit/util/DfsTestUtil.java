package com.knownstylenolife.hadoop.workshop.unit.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

import com.google.common.io.Files;

public class DfsTestUtil {

	private static Log LOG = LogFactory.getLog(DfsTestUtil.class.getName());
	
	public static void cleanDirs(Path inDir, Path outDir, FileSystem fs) throws IOException {
		if (fs.exists(inDir)) {
			fs.delete(inDir, true);
		}
		fs.mkdirs(inDir);
		if (fs.exists(outDir)) {
			fs.delete(outDir, true);
		}
	}

	public static void uploadLocalFileToInputDir(FileSystem fs, Path inDir, File... inputFiles) throws IOException {
		for (File file : inputFiles) {
			if (!file.exists()) {
				throw new RuntimeException("Local file does not exsists. filename =" + file.getAbsolutePath());
			}
			LOG.info("Upload local file. filename = " + file.getAbsolutePath());
			fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath()),
					new Path(inDir, file.getName()));
		}
	}

	public static void createInputFiles(Path inDir, FileSystem fs, String... inputs) throws IOException {
		int numInputFiles = inputs.length;
		for (int i = 0; i < numInputFiles; ++i) {
			DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
			file.writeBytes(inputs[i]);
			file.close();
		}
	}

	public static String readOutputsToString(Path outDir, Configuration conf) throws IOException {
		return MapReduceTestUtil.readOutput(outDir, conf);
	}
	
	public static Path[] getOutputFiles(Path outDir, FileSystem fs) throws IOException {
		return FileUtil.stat2Paths(fs.listStatus(outDir, new OutputFilesFilter()));
	}
	
}
