package com.knownstylenolife.hadoop.workshop.common.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {

	private static Log LOG = LogFactory.getLog(HdfsUtil.class);

	public static void copyFromLocalFile(String srcFilePath, String dstFilePath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path src = new Path(srcFilePath);
		Path dst = new Path(dstFilePath);
		if(fs.exists(dst)){
			LOG.info("The dst path already exists. path = " + dst.toString());
		}
		else{
			fs.copyFromLocalFile(src, dst);
		}
	}
	
	public static void mkdirs(String dirPath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(dirPath);
		if(fs.exists(path)){
			LOG.info("The path already exists. path = " + path.toString());
		}
		else{
			fs.mkdirs(path);
		}
	}
	
	public static void deleteDirectoryContents(String dirPath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		HdfsUtil.deleteDirectoryContents(fs, new Path(dirPath));
	}
	
	private static void deleteDirectoryContents(FileSystem fs, Path dirPath) throws IOException {
		if(fs.isFile(dirPath)) {
			throw new RuntimeException("The path is not directory. path = " + dirPath.toString());
		}
		else{
			FileStatus[] fileStatuses = fs.listStatus(dirPath);
			if(fileStatuses != null) {
				for(FileStatus fileStatus : fileStatuses) {
					HdfsUtil.deleteRecursively(fs, fileStatus.getPath());
				}
			}
		}
	}
	
	public static void deleteRecursively(String path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		HdfsUtil.deleteRecursively(fs, new Path(path));
	}
	
	private static void deleteRecursively(FileSystem fs, Path path) throws IOException {
		if(fs.isFile(path)) {
			fs.delete(path, false);
		}
		else {
			HdfsUtil.deleteDirectoryContents(fs, path);			
		}
	}
	
	public static void deleteFileIfExists(String path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path dstPath = new Path(path);
		if(fs.exists(dstPath)) {
			fs.delete(dstPath, false);
		}
	}
	
	public static void write(String contents, String path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path dstPath = new Path(path);
		if(fs.exists(dstPath)) {
			throw new RuntimeException("The path is already exists. path = " + dstPath.toString());
		}
		FSDataOutputStream out = fs.create(dstPath);
		out.writeUTF(contents);
		out.close();
	}
}
