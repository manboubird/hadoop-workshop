package com.knownstylenolife.hadoop.workshop.count.tool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.comparator.PerSplitSemiJoinWordKeyGroupComparator;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.PerSplitSemiJoinMstDataMapper;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.PerSplitSemiJoinReducer;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.PerSplitSemiJoinWordCountMapper;
import com.knownstylenolife.hadoop.workshop.count.partitioner.PerSplitSemiJoinWordPartitioner;
import com.knownstylenolife.hadoop.workshop.count.writable.PerSplitSemiJoinMapOutputKeyWritable;


public class PerSplitSemiJoinToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(PerSplitSemiJoinToolMain.class);
  
	private static final String CACHE_DIR_PATH = "/cache";

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		LogUtil.setLastArgAsLogLevel(args, conf);
		Path wordCountDataInputPath = new Path(args[0]);
		Path mstDataInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		File mstDataLocalFile = new File(args[3]);
		LOG.info("wordCountDataInputPath = " + HdfsUtil.makeQualifedPath(wordCountDataInputPath).toString()); 
		LOG.info("mstDataInputPath       = " + HdfsUtil.makeQualifedPath(mstDataInputPath).toString());
		LOG.info("outputPath             = " + HdfsUtil.makeQualifedPath(outputPath).toString());
		LOG.info("mstDataLocalFilePath   = " + mstDataLocalFile.getAbsolutePath());
		prepare(conf, mstDataLocalFile);
		return getSubmittableJob(conf, wordCountDataInputPath, mstDataInputPath, outputPath)
				.waitForCompletion(true) ? 0 : 1;
	}
	
	public void prepare(Configuration conf, File mstDataLocalFile) throws URISyntaxException, IOException {
		// cleanup Hdfs cache directory 
		HdfsUtil.mkdirs(CACHE_DIR_PATH);
		HdfsUtil.deleteDirectoryContents(CACHE_DIR_PATH);
	
		// create a zip file consisting of splitted links.txt on Hdfs
		File splitJoinZipFile = createSplitJoinZip(
				mstDataLocalFile, 
				300,//64 * 1024 * 1024, 
				"UTF-8");
		
		// upload zip file to Hdfs
		InputStream is = Resources.newInputStreamSupplier(splitJoinZipFile.toURI().toURL()).getInput();
		Path splitJoinFilePath = new Path(CACHE_DIR_PATH + "/splitJoinFiles.zip");
		FSDataOutputStream os = HdfsUtil.getFileSystem().create(splitJoinFilePath);
		copyStream(is, os, 1024);
		if (is != null) { try { is.close(); } catch (IOException e) { throw new RuntimeException(e); } }
		if (os != null) { try { os.close(); } catch (IOException e) { throw new RuntimeException(e); } }
		
		// configure DistributedCache
		String symLinkName = "splitJoinFiles";
		URI uri = new URI(splitJoinFilePath.toString() + "#" + symLinkName);
		LOG.info("Distributed cache archive uri = " + uri.toString());
		DistributedCache.addCacheArchive(uri, conf);
		DistributedCache.createSymlink(conf);
		conf.set(PerSplitSemiJoinWordCountMapper.class.getName() + ".joinFiles", symLinkName);
	}
	
	public File createSplitJoinZip(File splitFile, int maxFileSizePerFile, String charset) {
		File zipFile = null;
		try {
			zipFile = createZip(splitIntoFilesByLine(splitFile, maxFileSizePerFile, charset), charset);
		} catch (IOException e) {
			LOG.error(e);
		}
		return zipFile;
	}
	
	private File[] splitIntoFilesByLine(File file, int maxFileLength, String charset) throws IOException {
		Preconditions.checkState(maxFileLength > 30);
		List<File> list = new ArrayList<File>();
		BufferedReader reader = null;
		BufferedWriter writer = null;
	    try {
	    	reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
		    int fileCount = 0;
			File writtenFile = File.createTempFile(file.getName() + "." + String.valueOf(++fileCount) + ".", ".txt");
			list.add(writtenFile);
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(writtenFile), charset));
			String line;
		    int lengthCount = 0;
		    while((line = reader.readLine()) != null) {
		    	int length = line.getBytes(charset).length;
		    	if(lengthCount + length > maxFileLength) {
		    		writer.close();
		    		lengthCount = 0;
		    		writtenFile = File.createTempFile(file.getName() + "." + String.valueOf(++fileCount) + ".", ".txt");
		    		list.add(writtenFile);
		    		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(writtenFile), charset));
		    	}
		    	writer.write(line + "\n");
		    	lengthCount += length;
		    }
	    }finally {
            if(reader != null) { reader.close(); }
            if(writer != null) { writer.close(); }
	    }
	    return list.toArray(new File[]{});
	}

	private static byte[] buf = new byte[1024];

	private File createZip(File[] files, String charset) throws IOException {
		File tmpFile = File.createTempFile("data", ".zip");
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmpFile));
		zos.setEncoding(charset);
		try {
			for(File file : files) {
				ZipEntry ze = new ZipEntry(file.getName());
				zos.putNextEntry(ze);
				InputStream is = new BufferedInputStream(new FileInputStream(file));
				for (;;) {
					int len = is.read(buf);
					if (len < 0) break;
					zos.write(buf, 0, len);
				}
				if(is != null) { is.close(); }
			}
		} finally {
			if(zos != null) { zos.close(); }
		}
		return tmpFile;
	}
	
	private void copyStream(InputStream in, DataOutputStream os, int bufferSize) throws IOException {
		int len = -1;
		byte[] b = new byte[bufferSize * 1024];
		while ((len = in.read(b, 0, b.length)) != -1) {
			os.write(b, 0, len);
		}
		os.flush();
	}
	
	public Job getSubmittableJob(Configuration conf, Path wordCountDataInputPath, Path mstDataInputPath, Path outputPath) throws IOException {
		Job job = new Job(conf, "Per-Split Semi Join");
		job.setJarByClass(PerSplitSemiJoinToolMain.class);

		MultipleInputs.addInputPath(job, wordCountDataInputPath, TextInputFormat.class, PerSplitSemiJoinWordCountMapper.class);
		MultipleInputs.addInputPath(job, mstDataInputPath, TextInputFormat.class, PerSplitSemiJoinMstDataMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(PerSplitSemiJoinReducer.class);

        job.setMapOutputKeyClass(PerSplitSemiJoinMapOutputKeyWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(PerSplitSemiJoinMapOutputKeyWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(PerSplitSemiJoinWordKeyGroupComparator.class);
        job.setPartitionerClass(PerSplitSemiJoinWordPartitioner.class);
        
        return job;
	}

    public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PerSplitSemiJoinToolMain(), args));
    }
}
