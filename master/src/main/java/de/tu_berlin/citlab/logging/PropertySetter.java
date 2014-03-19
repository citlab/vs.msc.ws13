package de.tu_berlin.citlab.logging;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertySetter {

	private static String[] fields = {"serverName", "serverPort", "databaseName", "tableName", "user", "pass"};
	
	private static final String defaultPropertiesFile = "/log4j2_mysql.properties";
	
	public static void setLog4j2Properties() {
		setLog4j2Properties(defaultPropertiesFile);
	}
	
	public static void setLog4j2Properties(String fileNameInClassPathRoot) throws RuntimeException {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = PropertySetter.class.getResourceAsStream(fileNameInClassPathRoot);
			prop.load(input);
			for(String field : fields) {
				String value = prop.getProperty(field);
				if(value != null) {
					System.setProperty(String.format("log4j2.%s", field), value);
				}
				else {
					throw new Exception(String.format("Required field '%s' was not found in properties file (%s)", field, fileNameInClassPathRoot));
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
	
	public static void main(String[] args) {
		setLog4j2Properties();
		for(String field : fields) {
			System.out.println(System.getProperty(String.format("log4j2.%s", field)));
		}
	}
	
}
