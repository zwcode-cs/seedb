package core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {
	private static Connection connection = null;
	private static String database = "127.0.0.1/donations";
	
	public static void ConnectToDatabase(String s) {
		database = s;
		Instantiate();
	}
	
	public static void Instantiate() {
		if (connection == null) {
			//find driver
			try {
			    Class.forName("org.postgresql.Driver");
			    System.out.println("DB driver found");
			} catch (ClassNotFoundException e) {
			    System.out.println("DB driver not found");
			    e.printStackTrace();
			    return;
			}
			
			//attempt to connect
			try {
				 
				connection = DriverManager.getConnection(
						"jdbc:postgresql://" + database, "postgres",
						"postgrespwd");
	 
			} catch (SQLException e) {
	 
				System.out.println("DB connection failed.");
				e.printStackTrace();
				return;
			}
		 
			if (connection == null) {
				System.out.println("DB connection failed.");
			}
		}		
	}
	
	private QueryExecutor() {}
	
	public static ResultSet executeQuery(String query)
	{
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// Get a statement from the connection
		    stmt = connection.createStatement() ;

		    // Execute the query
		    rs = stmt.executeQuery(query) ;
		}
		catch(Exception e)
		{
			System.out.println("Error in executing query");
			e.printStackTrace();
		}
		return rs;
	}
	
	public static ResultSet getTableColumns(String table) {
		if (table == null) {
			throw new NullPointerException("Table is null.");
		}
		DatabaseMetaData dbmd = null;
		ResultSet rs = null;
		try {
			dbmd = connection.getMetaData();
			rs = dbmd.getColumns(null, null, table, null);
		} catch (SQLException e) {
			System.out.println("Error in executing query");
			e.printStackTrace();
			return null;
		}
		return rs;
	}
}
