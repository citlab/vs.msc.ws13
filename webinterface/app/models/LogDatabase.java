package models;

import play.*;
import play.mvc.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.ArrayList;

public class LogDatabase {

  private static final LogDatabase INSTANCE = new LogDatabase();

  public static LogDatabase getInstance() {
    return INSTANCE;
  }

  private String getConnectionString() {
    DatabaseConfig prop = DatabaseConfig.getInstance();

    return "jdbc:mysql://" + prop.getProperty("host_url") + ":"
        + prop.getProperty("host_port") + "/"
        + prop.getProperty("database") + "?user="
        + prop.getProperty("u_log") + "&password="
        + prop.getProperty("p_log");
  }

  public ArrayList<LogEntry> getLogs(long lastId) {
    Connection conn = null;
    PreparedStatement stmt = null;

    ArrayList<LogEntry> list = new ArrayList<LogEntry>();

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM log4j2 WHERE id>?");
      stmt.setLong(1, lastId);

      ResultSet rs = stmt.executeQuery();

      while(rs.next()) {
        list.add(new LogEntry(rs.getInt("id"))
          .datetime(rs.getTimestamp("datetime").toString())
          .milliseconds(new Integer(rs.getInt("milliseconds")).toString())
          .logger(rs.getString("logger"))
          .level(rs.getString("level"))
          .message(rs.getString("message"))
          .exception(rs.getString("exception"))
          .thread(rs.getString("thread"))
          .marker(rs.getString("marker"))
        );
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null)
          stmt.close();
      } catch (SQLException e) {
      }
      try {
        if (conn != null)
          conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    return list;
  }

  public LogEntry getLog(long id) {
    Connection conn = null;
    PreparedStatement stmt = null;

    LogEntry entry = null;
    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM log4j2 WHERE id=?");
      stmt.setLong(1, id);

      ResultSet rs = stmt.executeQuery();
      rs.first();
      entry = new LogEntry(rs.getInt("id"))
        .datetime(rs.getTimestamp("datetime").toString())
        .milliseconds(new Integer(rs.getInt("milliseconds")).toString())
        .logger(rs.getString("logger"))
        .level(rs.getString("level"))
        .message(rs.getString("message"))
        .exception(rs.getString("exception"))
        .thread(rs.getString("thread"))
        .marker(rs.getString("marker"));
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null)
          stmt.close();
      } catch (SQLException e) {
      }
      try {
        if (conn != null)
          conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    return entry;
  }

  public int truncateLogs() {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("TRUNCATE log4j2");

      ResultSet rs = stmt.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null)
          stmt.close();
      } catch (SQLException e) {
      }
      try {
        if (conn != null)
          conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return 0;
  }
}
