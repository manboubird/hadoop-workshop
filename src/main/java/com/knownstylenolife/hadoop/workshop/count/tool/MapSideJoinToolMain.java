package com.knownstylenolife.hadoop.workshop.count.tool;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.MapSideJoinReducer;
import com.knownstylenolife.hadoop.workshop.count.mapreduce.MapSideJoinWordCountMapper;
import com.knownstylenolife.hadoop.workshop.count.writable.MapSideJoinMapOutputValueWritable;


public class MapSideJoinToolMain extends Configured implements Tool {

	Log LOG = LogFactory.getLog(MapSideJoinToolMain.class);
    
	private static final String CACHE_DIR_PATH = "/tmp/cache/";

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		LogUtil.setLastArgAsLogLevel(args, conf);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		URL mstDataUrl = Resources.getResource("LinkDb.zip"); 

		LOG.info("inputPath  = " + HdfsUtil.makeQualifedPath(inputPath).toString()); 
		LOG.info("outputPath = " + HdfsUtil.makeQualifedPath(outputPath).toString());
		LOG.info("mstDataUrl = " + mstDataUrl.toString());

		// cleanup cache directory
		HdfsUtil.mkdirs(CACHE_DIR_PATH);
		HdfsUtil.deleteDirectoryContents(CACHE_DIR_PATH);
		
		// upload LinkDb.zip
		InputStream is = Resources.newInputStreamSupplier(mstDataUrl).getInput();
		FSDataOutputStream os = HdfsUtil.getFileSystem().create(new Path(CACHE_DIR_PATH + "/LinkDb.zip"));
		copyStream(is, os, 1024);
		
		// configure distributed cache
		URI uri = new URI(CACHE_DIR_PATH + "LinkDb.zip#LinkDb.zip");
		LOG.info("Distributed cache archive uri = " + uri.toString());
		DistributedCache.addCacheArchive(uri, conf);
		DistributedCache.createSymlink(conf);

		Job job = new Job(conf, "Map-side Join");
		job.setJarByClass(getClass());

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MapSideJoinWordCountMapper.class);
        job.setReducerClass(MapSideJoinReducer.class);
        job.setCombinerClass(MapSideJoinReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapSideJoinMapOutputValueWritable.class);
        
//        job.setNumReduceTasks(2);
        return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void copyStream(InputStream in, DataOutputStream os, int bufferSize) throws IOException {
		int len = -1;
		byte[] b = new byte[bufferSize * 1024];
		try {
			while ((len = in.read(b, 0, b.length)) != -1) {
				os.write(b, 0, len);
			}
			os.flush();
		} finally {
			if (in != null) { try { in.close(); } catch (IOException e) { throw new RuntimeException(e); } }
			if (os != null) { try { os.close(); } catch (IOException e) { throw new RuntimeException(e); } }
		}
	}

    public static void main( String[] args) throws Exception {
		System.exit(ToolRunner.run(new MapSideJoinToolMain(), args));
    }
}
