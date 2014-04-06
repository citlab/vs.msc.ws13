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

public class FileDatabase {

  private static final FileDatabase INSTANCE = new FileDatabase();

  public static FileDatabase getInstance() {
    return INSTANCE;
  }

  private String getConnectionString() {
    DatabaseConfig prop = DatabaseConfig.getInstance();

    return "jdbc:mysql://" + prop.getProperty("host_url") + ":"
        + prop.getProperty("host_port") + "/"
        + prop.getProperty("database") + "?user="
        + prop.getProperty("u_web") + "&password="
        + prop.getProperty("p_web");
  }

  public String addFile(String title, String name, String user) {
    Connection conn = null;
    PreparedStatement stmt = null;
    boolean result = false;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("INSERT INTO files(title,name,user) VALUES(?,?,?)");

      stmt.setString(1, title);
      stmt.setString(2, name);
      stmt.setString(3, user);

      stmt.executeUpdate();

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

    return "0";
  }

  public ArrayList<Topology> getFilesForUser(String user) {
    Connection conn = null;
    PreparedStatement stmt = null;

    ArrayList<Topology> list = new ArrayList<Topology>();

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM files WHERE user=?");

      stmt.setString(1, user);

      ResultSet rs = stmt.executeQuery();

      while(rs.next()) {
        list.add(new Topology(rs.getString("title"), user, rs.getTimestamp("date").toString(), rs.getString("name")));
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

  public ArrayList<Topology> getAllFiles() {
    Connection conn = null;
    PreparedStatement stmt = null;

    ArrayList<Topology> list = new ArrayList<Topology>();

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM files");

      ResultSet rs = stmt.executeQuery();

      while(rs.next()) {
        list.add(new Topology(rs.getString("title"), rs.getString("user"), rs.getTimestamp("date").toString(), rs.getString("name")));
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

  public String deleteFile(String title) {
    Connection conn = null;
    PreparedStatement stmt = null;
    boolean result = false;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("DELETE FROM files WHERE title=?");

      stmt.setString(1, title);

      stmt.executeUpdate();

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

    return title;
  }
}
