package de.tu_berlin.citlab.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DatabaseConfig {
	private static final DatabaseConfig INSTANCE = new DatabaseConfig();

	public static DatabaseConfig getInstance() {
		return INSTANCE;
	}

	private final Properties prop;

	private DatabaseConfig() {
		prop = new Properties();
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(
					"/home/ubuntu/StormRegisterService/db.properties");
			prop.load(fis);
		} catch (IOException e) {
			System.err.println("Could not load database configuration file");
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public String getProperty(String name) {
		return prop.getProperty(name);
	}
}
