package de.tu_berlin.citlab.logging;

import java.sql.Connection;
import java.sql.SQLException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;


public class ConnectionFactory {
	
	private static ConnectionFactory instance = null;
	
	public static ConnectionFactory getInstance() {
		if(instance == null) {
			instance = new ConnectionFactory();
		}
		return instance;
	}
	
	// TODO: set by config file
	private String serverName = "192.168.56.101";
	private String serverPort = "3306";
	private String databaseName = "log4j2";
	private String tableName = "citstorm";
	private String user = "log4j2";
	private String pass = "log4j2";
	
	private BoneCP connectionPool = null;
	
	private ConnectionFactory() {
		if(databaseDriverLoadable()) {
			try {
				BoneCPConfig config = getConfig();
				if(config != null) {
					connectionPool = new BoneCP(getConfig());
				}
				else {
					System.out.println("config is null");
				}
				//connectionPool.shutdown();
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
			System.out.println("driver not loadable");
			e.printStackTrace();
			result = false;
		}
		return result;
	}
	
	private BoneCPConfig getConfig() {
		// TODO: load from config file
		BoneCPConfig config = new BoneCPConfig();
		config.setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s", serverName, serverPort, databaseName));
		config.setUsername(user); 
		config.setPassword(pass);
		config.setMinConnectionsPerPartition(5);
		config.setMaxConnectionsPerPartition(10);
		config.setPartitionCount(1);
		return config;
	}

	public Connection getConnection() {
		Connection result = null;
		try {
			result = connectionPool.getConnection();
		} catch (SQLException e) {
			System.out.println("error");
			e.printStackTrace();
		}
		return result;
	}
	
	

	public static Connection getConnectionStatic() {
		return getInstance().getConnection();
	}
	
	public static void main(String[] args) {
		getConnectionStatic();
	}
	
}
