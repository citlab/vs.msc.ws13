/**
 * A simple utility class to load twitter login credentials from a file.
 */

package models.aws;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

  private static final Config INSTANCE = new Config();

  public static Config getInstance() {
    return INSTANCE;
  }

  private final Properties prop;

  private Config() {
    prop = new Properties();
    FileInputStream fis = null;

    try {
      fis = new FileInputStream("conf/manager.properties");
      prop.load(fis);
    } catch (IOException e) {
      System.err.println("Could not load manager configuration file");
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
