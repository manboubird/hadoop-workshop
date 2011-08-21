package com.knownstylenolife.hadoop.workshop.common.service;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.knownstylenolife.hadoop.workshop.common.dao.DbcpConnectionManager;
import com.knownstylenolife.hadoop.workshop.common.dao.LinkDao;

public class LinkDataService {

	public void prepareDatabase(String dbFilePath) throws Exception {
		File file = new File(dbFilePath);
		if(!file.exists()) {
			throw new RuntimeException("File not found. " + file.getAbsolutePath());
		}
		DbcpConnectionManager.init(file, false);
	}
	
	public void shutDownDatabase() throws Exception {
		DbcpConnectionManager.shutdownDriver();
	}
	
	/**
	 * import link.txt into derby
	 * @throws Exception 
	 */
	public void importLinks2Db(String filePath, String dbFilePath) throws Exception {
		File file = new File(filePath);
		Preconditions.checkState(file.exists(), "File not found. " + file.getAbsolutePath());
		Connection conn = null;
		PreparedStatement pStmt = null;
		try {
			DbcpConnectionManager.init(new File(dbFilePath), true);
			LinkDao.createTable();
			conn = DbcpConnectionManager.getConnection();
			pStmt = conn.prepareStatement("INSERT INTO link (word, url) VALUES(?, ?)");
			for(String line : Files.readLines(file, Charsets.US_ASCII)) {
				Iterable<String> it = Splitter.on("\t").omitEmptyStrings().trimResults().split(line);
				Preconditions.checkState(Iterables.size(it) == 2, "Illegal line format. \"" + line + "\"");
				Iterator<String> itr = it.iterator();
				pStmt.setString(1, itr.next()); // word
				pStmt.setString(2, itr.next()); // link
				pStmt.addBatch();
			}
			pStmt.executeBatch();
			conn.commit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}finally{
			try { if(conn != null) conn.close(); } catch (SQLException e) {}
			try { if(pStmt != null) pStmt.close(); } catch (SQLException e) {}
			DbcpConnectionManager.shutdownDriver();
		}
	}
	
	public String getMstData(String word) {
		return LinkDao.selectUrlByWord(word);
	}
}
