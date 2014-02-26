package microbenchmarks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Maps;

public class ConnectionPool {
	private int nDBConnections;
	private final HashMap<Connection, Integer> connectionsInUse;
	private final Semaphore available;
	
	public ConnectionPool(int nDBConnections, String dbType, String dbAddress, 
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
			    Class.forName("org." + dbType + ".Driver");
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
			} catch (SQLException e) {
				connection = null;
				System.out.println("DB connection failed.");
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
		for (Connection c : connectionsInUse.keySet()) {
			c.close();
		}
	}

}
