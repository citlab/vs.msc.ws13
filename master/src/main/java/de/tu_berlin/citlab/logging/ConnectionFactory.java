package de.tu_berlin.citlab.logging;

import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class ConnectionFactory {

	private static ConnectionFactory instance = null;

	public static ConnectionFactory getInstance() {
		if (instance == null) {
			instance = new ConnectionFactory();
		}
		return instance;
	}

	// overridden by config file
	private String serverName = "citstorm.dd-dns.de";
	private String serverPort = "3306";
	private String databaseName = "citstorm";
	private String tableName = "log4j2";
	private String user = "log4j2";
	private String pass = "log4j2";

	private BoneCP connectionPool;

	private ConnectionFactory() {
		if (databaseDriverLoadable() /*&& propertiesLoaded()*/) {
			try {
				connectionPool = new BoneCP(getConfig());
				// connectionPool.shutdown();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean databaseDriverLoadable() {
		boolean result = true;
		try {
			// load the database driver (make sure this is in your classpath!)
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			e.printStackTrace();
			result = false;
		}
		return result;
	}

	private BoneCPConfig getConfig() {
		BoneCPConfig config = new BoneCPConfig();
		config.setInitSQL("SET CHARACTER SET utf8mb4, character_set_client = utf8mb4, character_set_connection = utf8mb4, " +
                "character_set_results = utf8mb4, NAMES utf8mb4, collation_connection = utf8mb4_general_ci, " +
                "collation_database = utf8mb4_general_ci");
		config.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s",
				serverName, serverPort, databaseName));
		config.setUsername(user);
		config.setPassword(pass);
		config.setMinConnectionsPerPartition(5);
		config.setMaxConnectionsPerPartition(10);
		config.setPartitionCount(1);
		return config;
	}

	private boolean propertiesLoaded() {
		boolean result = true;
		try {
			serverName = System.getProperty("log4j2.serverName");
			serverPort = System.getProperty("log4j2.serverPort");
			databaseName = System.getProperty("log4j2.databaseName");
			tableName = System.getProperty("log4j2.tableName");
			user = System.getProperty("log4j2.user");
			pass = System.getProperty("log4j2.pass");
			if(serverName == null || serverPort == null || databaseName == null || tableName == null || user == null || pass == null) {
				throw new Exception(String.format("Invalid db credentials: serverName='%s', serverPort='%s', databaseName='%s', tableName= '%s', user='%s', pass='%s'", serverName, serverPort, databaseName, tableName, user, pass));
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
			result = false;
		}
		return result;
	}

	public Connection getConnection() {
		Connection result = null;
		try {
			result = connectionPool.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static Connection getConnectionStatic() {
		return getInstance().getConnection();
	}

}
