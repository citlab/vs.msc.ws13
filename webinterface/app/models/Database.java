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

public class Database {

  private static final Database INSTANCE = new Database();

  public static Database getInstance() {
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

  private String generateSalt(int length) {
    final Random r = new SecureRandom();
    byte[] salt = new byte[length];
    r.nextBytes(salt);

    // converting byte array to Hexadecimal String
    StringBuilder sb = new StringBuilder(2 * salt.length);
    for (byte b : salt) {
      sb.append(String.format("%02x", b & 0xff));
    }

    return sb.toString();
  }

  private String md5(String message) {
    String result = null;

    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] hash = md.digest(message.getBytes("UTF-8"));

      // converting byte array to Hexadecimal String
      StringBuilder sb = new StringBuilder(2 * hash.length);
      for (byte b : hash) {
        sb.append(String.format("%02x", b & 0xff));
      }

      result = sb.toString();

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return result;
  }

  public boolean doesUserExist(String name) {
    Connection conn = null;
    PreparedStatement stmt = null;

    boolean result = false;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn
          .prepareStatement("SELECT COUNT(*) FROM users WHERE name = ?");
      stmt.setString(1, name);

      ResultSet rs = stmt.executeQuery();
      rs.first();
      if (rs.getInt(1) > 0) {
        result = true;
      }

      rs.close();
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

    return result;
  }

  public int createUser(String name, String password) {
    Connection conn = null;
    PreparedStatement stmt = null;

    int result = -1;

    if (doesUserExist(name)) {
      result = 1;
    } else {
      try {
        // Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

        // Open a connection
        conn = DriverManager.getConnection(getConnectionString());

        // Execute SQL query
        stmt = conn
            .prepareStatement("INSERT INTO users(name,password,salt) VALUES(?, ?, ?)");

        String salt = generateSalt(16);
        String hash = md5(password + salt);

        stmt.setString(1, name);
        stmt.setString(2, hash);
        stmt.setString(3, salt);

        stmt.executeUpdate();

        result = 0;

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
    }

    return result;
  }

  /**
   * Check if login credentials are valid
   * 
   * @param name
   *            The user name
   * @param password
   *            The user password in cleartext
   * @return returns true, if the user name and password match
   */
  public boolean validateUser(String name, String password) {
    Connection conn = null;
    PreparedStatement stmt = null;

    boolean result = false;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM users WHERE name = ?");

      stmt.setString(1, name);

      ResultSet rs = stmt.executeQuery();

      if (rs.first()) {
        String salt = rs.getString("salt");
        String hash = rs.getString("password");
        result = md5(password + salt).equals(hash);
      }

      rs.close();

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

    return result;
  }

  public String updateSession(String name, String sessionId) {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn
          .prepareStatement("UPDATE users SET session = ? WHERE name = ?");

      stmt.setString(1, sessionId);
      stmt.setString(2, name);

      stmt.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
      return "-1";
    } catch (Exception e) {
      e.printStackTrace();
      return "-1";
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

  public boolean checkSession(String name, String sessionId) {
    Connection conn = null;
    PreparedStatement stmt = null;
    boolean result = false;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM users WHERE name = ?");

      stmt.setString(1, name);

      ResultSet rs = stmt.executeQuery();

      if (rs.first()) {
        String session = rs.getString("session");
        result = session.equals(sessionId);
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

    return result;
  }

  public String getIP(int type) {
    Connection conn = null;
    PreparedStatement stmt = null;

    String result = null;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM nodes WHERE type = ?");

      stmt.setInt(1, type);

      ResultSet rs = stmt.executeQuery();

      if (rs.first()) {
        result = rs.getString("ip");
      }

      rs.close();

    } catch (SQLException e) {
      e.printStackTrace();
      return "-1";
    } catch (Exception e) {
      e.printStackTrace();
      return "-1";
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

    return result;
  }

  public String updateIP(int type, String ip) {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn
          .prepareStatement("UPDATE nodes SET ip = ? WHERE type = ?");

      stmt.setString(1, ip);
      stmt.setInt(2, type);

      stmt.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
      return "-1";
    } catch (Exception e) {
      e.printStackTrace();
      return "-1";
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

  public ArrayList<User> getUsers() {
    Connection conn = null;
    PreparedStatement stmt = null;

    ArrayList<User> list = new ArrayList<User>();

    try {
      // Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      // Open a connection
      conn = DriverManager.getConnection(getConnectionString());

      // Execute SQL query
      stmt = conn.prepareStatement("SELECT * FROM users");

      ResultSet rs = stmt.executeQuery();

      while(rs.next()) {
        list.add(new User(
          rs.getString("name"),
          rs.getInt("uid"),
          rs.getString("session")));
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
}
