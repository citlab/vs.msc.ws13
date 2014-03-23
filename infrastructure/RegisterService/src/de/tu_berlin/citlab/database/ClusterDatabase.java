/**
 * A simple utility class to load cluster properties from a file.
 */

package de.tu_berlin.citlab.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.tu_berlin.citlab.cluster.Instance;

public class ClusterDatabase {

	private static final ClusterDatabase INSTANCE = new ClusterDatabase();

	public static ClusterDatabase getInstance() {
		return INSTANCE;
	}

	private ClusterDatabase() {
	}

	private String getConnectionString() {
		DatabaseConfig prop = DatabaseConfig.getInstance();

		return "jdbc:mysql://" + prop.getProperty("host_url") + ":"
				+ prop.getProperty("host_port") + "/"
				+ prop.getProperty("database") + "?user="
				+ prop.getProperty("u_web") + "&password="
				+ prop.getProperty("p_web");
	}

	public void updateInstance(Instance instance) {
		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			// Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// Open a connection
			conn = DriverManager.getConnection(getConnectionString());

			// Execute SQL query
			stmt = conn
					.prepareStatement("SELECT * FROM cluster WHERE instanceId = ?");

			stmt.setString(1, instance.getInstanceId());

			ResultSet rs = stmt.executeQuery();

			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
			}

			if (!rs.first()) {

				stmt = conn
						.prepareStatement("INSERT INTO cluster(instanceId, imageId, instanceType, launchTime, role, publicIp, state) values(?,?,?,?,?,?,?)");
				stmt.setString(1, instance.getInstanceId());
				stmt.setString(2, instance.getImageId());
				stmt.setString(3, instance.getInstanceType());
				stmt.setTimestamp(4, new Timestamp(instance.getLaunchTime()
						.getTime()));
				stmt.setInt(5, instance.getRole());
				stmt.setString(6, instance.getPublicIp());
				stmt.setInt(7, instance.getState());

				stmt.executeUpdate();
			} else {
				stmt = conn
						.prepareStatement("UPDATE cluster SET instanceId=?, imageId=?, instanceType=?, launchTime=?, role=?, publicIp=?, state=? WHERE instanceId=?");
				stmt.setString(1, instance.getInstanceId());
				stmt.setString(2, instance.getImageId());
				stmt.setString(3, instance.getInstanceType());
				stmt.setTimestamp(4, new Timestamp(instance.getLaunchTime()
						.getTime()));
				stmt.setInt(5, instance.getRole());
				stmt.setString(6, instance.getPublicIp());
				stmt.setInt(7, instance.getState());
				stmt.setString(8, instance.getInstanceId());

				stmt.executeUpdate();
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
	}

	public String getProperty(String name) {
		Connection conn = null;
		PreparedStatement stmt = null;

		String result = null;

		try {
			// Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// Open a connection
			conn = DriverManager.getConnection(getConnectionString());

			// Execute SQL query
			stmt = conn
					.prepareStatement("SELECT * FROM clustercfg WHERE name = ?");

			stmt.setString(1, name);

			ResultSet rs = stmt.executeQuery();

			if (rs.first()) {
				result = rs.getString("value");
			}

			rs.close();

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
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
}
