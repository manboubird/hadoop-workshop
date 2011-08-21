package com.knownstylenolife.hadoop.workshop.common.dao;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

public class DbcpConnectionManager {

	private static final String driver = "org.apache.derby.jdbc.EmbeddedDriver";

	public static void init(File dbFile, boolean isCreateDb) throws Exception {
		Class.forName(driver);
		String connectURI = "jdbc:derby:" + dbFile.getAbsolutePath();
		if(isCreateDb) connectURI += ";create=true";
		setupDriver(connectURI);
	}
	
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:apache:commons:dbcp:linkDb");
	}

	private static void setupDriver(String connectURI) throws Exception {
		ObjectPool connectionPool = new GenericObjectPool(null);
		ConnectionFactory connectionFactory 
			= new DriverManagerConnectionFactory(connectURI, null);
		new PoolableConnectionFactory(connectionFactory, connectionPool, null, null, false, true);
		Class.forName("org.apache.commons.dbcp.PoolingDriver");
		PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
		driver.registerPool("linkDb", connectionPool);
	}
	
    public static void shutdownDriver() throws Exception {
        PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        driver.closePool("linkDb");
    }
}
