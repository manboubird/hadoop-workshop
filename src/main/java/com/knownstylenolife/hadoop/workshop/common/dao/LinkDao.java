package com.knownstylenolife.hadoop.workshop.common.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LinkDao {

	public static void createTable() {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = DbcpConnectionManager.getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(
				"CREATE TABLE link (" + 
				"  link_id INT NOT NULL GENERATED ALWAYS AS IDENTITY," + 
				"  word VARCHAR(64) NOT NULL," + 
				"  url VARCHAR(1024) NOT NULL," + 
				"  PRIMARY KEY(link_id))");
			stmt.executeUpdate(
				"CREATE UNIQUE INDEX word_idx ON link(word)");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}finally{
			try { if(conn != null) conn.close(); } catch (SQLException e) {}
			try { if(stmt != null) stmt.close(); } catch (SQLException e) {}
		}
	}
	
	public static String selectUrlByWord(String word) {
		Connection conn = null;
		PreparedStatement pStmt = null;
		try {
			conn = DbcpConnectionManager.getConnection();
			pStmt = conn.prepareStatement("select url from link where word = ?");
			pStmt.setString(1, word);
			ResultSet rs = pStmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}finally{
			try { if(conn != null) conn.close(); } catch (SQLException e) {}
			try { if(pStmt != null) pStmt.close(); } catch (SQLException e) {}
		}
	}
}
