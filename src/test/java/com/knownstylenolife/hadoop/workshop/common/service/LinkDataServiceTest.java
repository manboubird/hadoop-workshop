package com.knownstylenolife.hadoop.workshop.common.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.knownstylenolife.hadoop.workshop.common.dao.DbcpConnectionManager;
import com.knownstylenolife.hadoop.workshop.common.dao.LinkDao;

public class LinkDataServiceTest {

	private LinkDataService derbyService;
	
	private String linkFilePath = "LinkDataServiceTest/testImportlinks2Db/links.txt";
	private File linkFile;
	private String derbyDirPath = "target/LinkDb";
	private File derbyDir;
	
	@Before
	public void setUp() throws Exception {
		derbyDir = new File(derbyDirPath);
		linkFile = new File(Resources.getResource(getClass(), linkFilePath).toURI());
		derbyService = new LinkDataService();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testImportLinks() throws Exception {
		if(derbyDir.exists()) {
			Files.deleteRecursively(derbyDir);
		}
		derbyService.importLinks2Db(linkFile.getAbsolutePath(), derbyDirPath);

		assertThat(derbyDir.exists(), is(true));
		DbcpConnectionManager.init(new File(derbyDirPath), false);
		for(String line : Files.readLines(linkFile, Charsets.US_ASCII)) {
			Iterator<String> itr = Splitter.on("\t").omitEmptyStrings().trimResults().split(line).iterator();
			String word = itr.next();
			String link = itr.next();
			assertThat(LinkDao.selectUrlByWord(word), is(link));
		}
		DbcpConnectionManager.shutdownDriver();
	}
	
	@Test
	public void testGetMstData() throws Exception {
//		derbyService.importLinks2Db(linkFile.getAbsolutePath(), derbyDirPath);
		derbyService.prepareDatabase(derbyDirPath);
		DbcpConnectionManager.init(new File(derbyDirPath), false);
		for(String line : Files.readLines(linkFile, Charsets.US_ASCII)) {
			Iterator<String> itr = Splitter.on("\t").omitEmptyStrings().trimResults().split(line).iterator();
			String word = itr.next();
			String link = itr.next();
			assertThat(derbyService.getMstData(word), is(link));
		}
		derbyService.shutDownDatabase();
	}
}
