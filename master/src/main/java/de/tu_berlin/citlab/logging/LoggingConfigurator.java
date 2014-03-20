package de.tu_berlin.citlab.logging;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.BaseConfiguration;

public class LoggingConfigurator {

	private static String[] fields = {"serverName", "serverPort", "databaseName", "tableName", "user", "pass"};
	
	private static final String defaultPropertiesFile = "/log4j2_mysql.properties";
	
	private static void setLog4j2Properties() {
		setLog4j2Properties(UUID.randomUUID().toString());
	}
	
	private static void setLog4j2Properties(String session) throws RuntimeException {
		System.setProperty("log4j2.session", session);
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = LoggingConfigurator.class.getResourceAsStream(defaultPropertiesFile);
			prop.load(input);
			for(String field : fields) {
				String value = prop.getProperty(field);
				if(value != null) {
					System.setProperty(String.format("log4j2.%s", field), value);
				}
				else {
					throw new Exception(String.format("Required field '%s' was not found in properties file (%s)", field, defaultPropertiesFile));
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					
				}
			}
		}
	}
	
	private static void addDatabaseAppender() {
		Logger coreLogger = (Logger) LogManager.getRootLogger();
		LoggerContext ctx = (LoggerContext) coreLogger.getContext();
		BaseConfiguration configuration = (BaseConfiguration) ctx.getConfiguration();
		coreLogger.addAppender(configuration.getAppender("databaseAppender"));
	}
	
	public static void activateDataBaseLogger() {
		setLog4j2Properties();
		addDatabaseAppender();
	}
	
	public static void activateDataBaseLogger(String session) {
		setLog4j2Properties(session);
		addDatabaseAppender();
	}
	
}
