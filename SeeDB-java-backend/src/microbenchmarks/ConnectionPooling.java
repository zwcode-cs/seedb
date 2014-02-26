package microbenchmarks;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import com.google.common.collect.Lists;

public class ConnectionPooling {	
	private ConnectionPool pool;
	private int nDBconnections;
	
	public ConnectionPooling(int nDBConnections, String dbType, String dbAddress, 
			String dbUser, String dbPassword) {
		this.pool = new ConnectionPool(nDBConnections, dbType, dbAddress, dbUser, dbPassword);
		this.nDBconnections = nDBConnections;
	}
	
	public Connection getConnectionFromPool() {
		Connection c = null;
		try {
			c = this.pool.getAvailableConnection();
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted while getting connection from pool");
			e.printStackTrace();
		}
		return c;
	}
	
	public void runQueriesOnMultipleConnections(String table) throws SQLException {
		// get connection for metadata about the table
		Connection c = getConnectionFromPool();
		if (c == null) return;
		
		DatabaseMetaData dbmd = null;
		ResultSet rs = null;
		try {
			dbmd = c.getMetaData();
			rs = dbmd.getColumns(null, null, table, null);
		} catch (SQLException e) {
			System.out.println("Error in executing metadata query");
			e.printStackTrace();
			return;
		}
		
		List<String> dimensionAttributes = Lists.newArrayList();
		List<String> measureAttributes = Lists.newArrayList();
		
		while (rs.next()) {
			String attribute = rs.getString("COLUMN_NAME");
			if (attribute.startsWith("dim")) dimensionAttributes.add(attribute);
			else if (attribute.startsWith("measure")) measureAttributes.add(attribute);
		}
		pool.returnConnectionToPool(c);
		
		Statement stmt = null;
		rs = null;
		try {
			stmt = c.createStatement();
			//stmt.execute("set max_connections=" + nDBconnections+1 + ";");
			System.out.println(stmt.execute("show max_connections;"));
			rs = stmt.getResultSet();
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long start = System.nanoTime();
		List<Thread> threads = Lists.newArrayList();
		// walk through all combinations and make query
		for (String dim : dimensionAttributes) {
			for (String measure : measureAttributes) {
				String sqlQuery = "SELECT SUM(" + measure + ") FROM " + table + " GROUP BY " + dim;
				// create a thread and execute query
				Thread t = new Thread(new ExecuteQuery(pool, sqlQuery));
				threads.add(t);
				t.start();
			}
		}
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				System.out.println("Thread join interrupted");
				e.printStackTrace();
			}
		}
		System.out.println("Total time: " + (System.nanoTime() - start));
		pool.closeAllConnections();
	}

	private class ExecuteQuery implements Runnable {
		private ConnectionPool pool;
		private String query;
		
		public ExecuteQuery(ConnectionPool pool, String query) {
			this.pool = pool;
			this.query = query;
		}
		
		public Connection getConnectionFromPool() {
			Connection c = null;
			try {
				c = this.pool.getAvailableConnection();
			} catch (InterruptedException e) {
				System.out.println("Thread interrupted while getting connection");
				e.printStackTrace();
			}
			return c;
		}
		
		@Override
		public void run() {
			//System.out.println("Executing: " + this.query);
			Connection c = getConnectionFromPool();
			if (c == null) {
				System.out.println("Null connection for thread");
			}
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = c.createStatement();
				long start = System.nanoTime();
			    rs = stmt.executeQuery(query);
			    System.out.println(nDBconnections + ", Time taken: " + (System.nanoTime() - start));
			} catch (Exception e) {
				System.out.println("Error in executing query");
			}
			pool.returnConnectionToPool(c);
		}
	}
}
