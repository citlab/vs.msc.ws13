/**
 * A simple utility class to load twitter login credentials from a file.
 */

package de.tu_berlin.citlab.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TwitterUserLoader {
	
        private static final Logger log = LogManager.getLogger(TwitterUserLoader.class); 

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
                        log.error(String.format("Could not load properties file from relative path '%s'. Not Existent or not readable.", path));
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

        /**
         * 
         * @param path
         *            The path to the properties file inside the jar archive
         * @return The properties object containing the loaded twitter credentials
         */
        public static Properties loadUserFromJar(String path) {
                Properties prop = new Properties();
                InputStream is = null;

                try {
                        is = TwitterUserLoader.class.getResourceAsStream("/" + path);
                        prop.load(is);

                } catch (IOException e) {
                        log.error(String.format("Could not load properties file from absolute class-path '/%s'. Not Existent or not readable.", path));
                } finally {
                        if (is != null) {
                                try {
                                        is.close();
                                } catch (IOException e) {
                                }
                        }
                }

                return prop;
        }
}