package com.knownstylenolife.hadoop.workshop.common.util;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;

public class HdfsUtil {

	private static Log LOG = LogFactory.getLog(HdfsUtil.class);
	
	private static Configuration conf;
	
	static {
		conf = new Configuration();
	}

	public static Configuration getConfiguration() {
		return conf;
	}
	
	public static void setConfiguration(Configuration conf) {
		HdfsUtil.conf = conf;
	}
	
	public static FileSystem getFileSystem() throws IOException {
		return FileSystem.get(HdfsUtil.getConfiguration());
	}
	
	public static URI getUri() throws IOException {
		return FileSystem.get(HdfsUtil.getConfiguration()).getUri();
	}
	
	public static void copyFromLocalFile(String srcFilePath, String dstFilePath) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		Path src = new Path(srcFilePath);
		Path dst = new Path(dstFilePath);
		if(fs.exists(dst)){
			LOG.info("The dst filename already exists. filename = " + dst.toString());
		}
		else{
			fs.copyFromLocalFile(src, dst);
		}
	}
	
	public static void mkdirs(String dirPath) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		Path path = new Path(dirPath);
		if(fs.exists(path)){
			LOG.info("The filename already exists. filename = " + path.toString());
		}
		else{
			fs.mkdirs(path);
		}
	}
	
	public static Path makeQualifedPath(Path path) throws IOException {
		return path.makeQualified(FileSystem.get(HdfsUtil.getConfiguration()));
	}
	
	public static Path[] getPathes(Path dirPath) throws IOException {
		return FileUtil.stat2Paths(FileSystem.get(HdfsUtil.getConfiguration()).listStatus(dirPath));
	}

	public static Path[] getOutputFiles(Path outDir) throws IOException {
		return FileUtil.stat2Paths(FileSystem.get(HdfsUtil.getConfiguration()).listStatus(outDir, new OutputFilesFilter()));
	}
	
	public static void deleteDirectoryContents(String dirPath) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		HdfsUtil.deleteDirectoryContents(fs, new Path(dirPath));
	}
	
	private static void deleteDirectoryContents(FileSystem fs, Path dirPath) throws IOException {
		if (fs.isFile(dirPath)) {
			throw new IllegalStateException("The filename is not directory. filename = " + dirPath.toString());
		}
		FileStatus[] fileStatuses = fs.listStatus(dirPath);
		if (fileStatuses == null) {
			throw new RuntimeException("Error listing files for " + dirPath.toString());
		}
		for (FileStatus fileStatus : fileStatuses) {
			HdfsUtil.deleteRecursively(fs, fileStatus.getPath());
		}
	}
	
	public static void deleteRecursivelyIfExists(String path) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		Path p = new Path(path);
		if(fs.exists(p)) {
			HdfsUtil.deleteRecursively(fs, p);
		}
	}
	
	public static void deleteRecursively(String path) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		HdfsUtil.deleteRecursively(fs, new Path(path));
	}
	
	private static void deleteRecursively(FileSystem fs, Path path) throws IOException {
		if(!fs.isFile(path)) {
			HdfsUtil.deleteDirectoryContents(fs, path);			
		}
		if(!fs.delete(path, false)) {
			throw new RuntimeException("Fail to delete " + path.toUri());
		}
	}
	
	public static void deleteFileIfExists(String path) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		Path dstPath = new Path(path);
		if(fs.exists(dstPath)) {
			fs.delete(dstPath, false);
		}
	}
	
	public static void write(String contents, String path) throws IOException {
		FileSystem fs = FileSystem.get(HdfsUtil.getConfiguration());
		Path dstPath = new Path(path);
		if(fs.exists(dstPath)) {
			throw new RuntimeException("The filename is already exists. filename = " + dstPath.toString());
		}
		FSDataOutputStream out = fs.create(dstPath);
		out.writeUTF(contents);
		out.close();
	}
}
