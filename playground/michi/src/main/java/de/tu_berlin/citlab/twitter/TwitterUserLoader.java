/**
 * A simple utility class to load twitter login credentials from a file.
 */

package de.tu_berlin.citlab.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TwitterUserLoader {

	/**
	 * 
	 * @param path
	 *            The path to the properties file
	 * @return The properties object containing the loaded twitter credentials
	 */
	public static Properties loadUser(String path) {
		Properties prop = new Properties();
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(path);
			prop.load(fis);
		} catch (IOException e) {
			System.err.println("Could not load properties file: " + path);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}

		return prop;
	}
}
