package com.knownstylenolife.hadoop.workshop.count.tool;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.common.util.HdfsUtil;
import com.knownstylenolife.hadoop.workshop.common.util.LogUtil;
import com.knownstylenolife.hadoop.workshop.unit.tool.MapReduceLocalTestCaseBase;
import com.knownstylenolife.hadoop.workshop.unit.util.DfsTestUtil;

public class PerSplitSemiJoinToolMainTest extends MapReduceLocalTestCaseBase {

	private Log LOG = LogFactory.getLog(PerSplitSemiJoinToolMainTest.class);
	
	private static final String MR_LOG_LEVEL = org.apache.log4j.Level.DEBUG.toString();

	private PerSplitSemiJoinToolMain tool;
	private String mstDataInputDirPath = "PerSplitSemiJoinToolMainTest/testRun_input/mstData";
	private File mstDataInputDir;
	private String wordCountDataInputDirPath = "PerSplitSemiJoinToolMainTest/testRun_input/wordCountData";
	private File wordCountDataInputDir;
	private String expectedOutputFilePath = "PerSplitSemiJoinToolMainTest/testRun_expected/part-r-00000";

	@Before
	public void setUp() throws Exception {
		mstDataInputDir = new File(Resources.getResource(getClass(), mstDataInputDirPath).toURI());
		wordCountDataInputDir = new File(Resources.getResource(getClass(), wordCountDataInputDirPath).toURI());
		tool = new PerSplitSemiJoinToolMain();
	}
	
	@After
	public void tearDown() throws Exception {
//		DistributedCache.
	}
	
	@Test
	public void testRun() throws Exception {
		prepareJobWithDirs(
			mstDataInputDir,
			wordCountDataInputDir);
		
		Configuration conf = getConfiguration();
		HdfsUtil.setConfiguration(conf);
		LogUtil.setLastArgAsLogLevel(new String[] { MR_LOG_LEVEL }, conf);
		Path wordCountDataInputPath = new Path(getInputDir(), wordCountDataInputDir.getName());
		Path mstDataInputPath = new Path(getInputDir(), mstDataInputDir.getName());
		Path outputPath = getOutputDir();
		File mstDataLocalFile = new File(mstDataInputDir, "links.txt");
		LOG.info("wordCountDataInputPath = " + HdfsUtil.makeQualifedPath(wordCountDataInputPath).toString()); 
		LOG.info("mstDataInputPath       = " + HdfsUtil.makeQualifedPath(mstDataInputPath).toString());
		LOG.info("outputPath             = " + HdfsUtil.makeQualifedPath(outputPath).toString());
		LOG.info("mstDataLocalFilePath   = " + mstDataLocalFile.getAbsolutePath());
		tool.prepare(conf, mstDataLocalFile);
		Job job = tool.getSubmittableJob(conf, wordCountDataInputPath, mstDataInputPath, outputPath);
	
		// TODO DC hack
//		for(URI archive : DistributedCache.getCacheArchives(conf)) {
//			Path localFile = new Path("target/files" + archive.getPath());
//			getFileSystem().copyToLocalFile(new Path(archive), localFile);
//			FileUtil.symLink(localFile.toString(), new File(".").getAbsolutePath() + Path.SEPARATOR + archive.getFragment());
//		}
		
//		URI[] archives = DistributedCache.getCacheArchives(conf);
//	    Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
//	    if(archives != null) {
//	    	for(int i = 0; i < archives.length; i++) {
////	    		String link = archives[i].getFragment();
//	    		String target = localArchives[i].toString();
//				FileUtil.symLink(target, new File(".").getAbsolutePath() + Path.SEPARATOR + archives[i].getFragment());
//			}
//	    }
		
		assertThat(job.waitForCompletion(true), is(true));
		
		LOG.info("OUTPUT:\n" + DfsTestUtil.readOutputsToString(getOutputDir(), getConfiguration()));
		
		Path[] outputFiles = DfsTestUtil.getOutputFiles(getOutputDir(), getFileSystem());
		// for local mode assertion. redueceTaskNum is set to 1.
		assertOutputFile(outputFiles[0], Resources.getResource(getClass(), expectedOutputFilePath));
	}

}
