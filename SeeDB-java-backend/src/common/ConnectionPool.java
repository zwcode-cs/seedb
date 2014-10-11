package common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Maps;

public class ConnectionPool {
	private int nDBConnections;
	private final HashMap<Connection, Integer> connectionsInUse;
	private final Semaphore available;
	
	public ConnectionPool(int nDBConnections, String dbAddress, String dbType, 
			String dbUser, String dbPassword) {
		this.nDBConnections = nDBConnections;
		this.connectionsInUse = this.createConnectionPool(dbType, dbAddress, dbUser, dbPassword);
		this.available = new Semaphore(nDBConnections, true);
	}
	
	public HashMap<Connection, Integer> createConnectionPool(String dbType, String dbAddress, String dbUser, 
			String dbPassword) {
		HashMap<Connection, Integer> connectionsInUse = Maps.newHashMap();
		for (int i = 0; i < nDBConnections; i++) {
			Connection connection = null;
			try {
				if (dbType.equalsIgnoreCase("PostgreSQL")) {
					Class.forName("org." + dbType + ".Driver");
				} else if (dbType.equalsIgnoreCase("vertica")) {
					Class.forName("com.vertica.jdbc.Driver");
				}
			} catch (ClassNotFoundException e) {
				connection = null;
			    System.out.println("DB driver not found");
			    e.printStackTrace();
			    return null;
			}
			
			//attempt to connect
			try {
				connection = DriverManager.getConnection(
						"jdbc:" + dbType + "://" + dbAddress, dbUser,
						dbPassword);
				Statement stmt = connection.createStatement();
				if (dbType.equalsIgnoreCase("PostgreSQL")) {
					stmt.execute("set work_mem=" + 1000000 + ";");
				}
			} catch (SQLException e) {
				connection = null;
				System.out.println("DB connection failed.");
				try {
					for (Connection c : connectionsInUse.keySet()) {
						c.close();
					}
				} catch (SQLException ex) {
					System.out.println("Couldn't close all connections");
					return null;
				}
				e.printStackTrace();
				return null;
			}
			connectionsInUse.put(connection, 0);
		}
		return connectionsInUse;
	}
	
	public void returnConnectionToPool(Connection c) {
		returnConnectionToPoolHelper(c);
		available.release();
	}
	
	public synchronized void returnConnectionToPoolHelper(Connection c) {
		connectionsInUse.put(c, 0);
	}
	
	public Connection getAvailableConnection() throws InterruptedException {
		available.acquire();
		return getAvailableConnectionHelper();
	}
	
	public synchronized Connection getAvailableConnectionHelper() {
		for (Connection c : connectionsInUse.keySet()) {
			if (connectionsInUse.get(c) == 0) {
				connectionsInUse.put(c, 1);
				return c;
			}
		}
		return null;
	}
	
	public void closeAllConnections() throws SQLException {
		if (connectionsInUse == null) return;
		for (Connection c : connectionsInUse.keySet()) {
			c.close();
		}
	}

}
