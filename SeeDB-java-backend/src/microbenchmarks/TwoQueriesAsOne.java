package microbenchmarks;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TwoQueriesAsOne {
	private int working_mem;
	private String table;
	private Connection connection;
	private String selectedDimAttribute;
	private int maxValue = 5000;
	private String selectAttribute;
	private String aggregateAttribute;
	
	public Connection getConnection(String dbType, String dbAddress, String dbUser, String dbPassword) {
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
		return connection;
	}
	
	public TwoQueriesAsOne(String table, int working_mem, String selectedDimAttribute, String selectAttribute, 
			String aggregateAttribute, String dbType, String dbAddress, String dbUser, String dbPassword) {
		this.table = table;
		this.working_mem = working_mem;
		this.connection = this.getConnection(dbType, dbAddress, dbUser, dbPassword);
		this.selectedDimAttribute = selectedDimAttribute;
		this.selectAttribute = selectAttribute;
		this.aggregateAttribute = aggregateAttribute;
	}
	
	public void runTest() throws SQLException {
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
		// pick two values for the selectedDimAttribute
		try {
			Statement stmt = null;
			stmt = connection.createStatement();
			stmt.execute("set work_mem=" + working_mem + ";");
			
		    double[] selectivities = new double[]{0.001, 0.01, 0.1, 0.2, 0.5, 0.7, 0.9};
		    for (double selectivity : selectivities) {
		    	double firstHalf = (1 - selectivity)/2;
		    	double lower = Math.random() * firstHalf;
		    	double upper = lower + selectivity*maxValue;
		    	String whereClause = "measure1 >= " + lower + " and measure1 <= " + upper;
		    	
		    	String query1 = "select " + selectedDimAttribute + ", count(" + aggregateAttribute + ") from " + table  
		    			+ " where " + whereClause + " group by " + selectedDimAttribute;
		    	String query2 = "select " + selectedDimAttribute + ", count(" + aggregateAttribute + ") from " + table 
		    			+ " group by " + selectedDimAttribute;
		    	String combined = "select " + selectedDimAttribute + ", case when " + whereClause + " then 0 else 1 end " +
		    			"as tmp from " + table + " group by " + selectedDimAttribute + ", tmp";
		    	long query1time = System.nanoTime();
		    	ResultSet rs = stmt.executeQuery(query1);
		    	query1time = (System.nanoTime() - query1time);
		    	
		    	long query2time = System.nanoTime();
			    rs = stmt.executeQuery(query2);
			    query2time = (System.nanoTime() - query2time);
			    
		    	long combinedtime = System.nanoTime();
		    	rs = stmt.executeQuery(combined);
		    	combinedtime = (System.nanoTime() - combinedtime);
			    
		    	System.out.println(table + "," + selectivity + "," + query1time * 1.0 /(long) Math.pow(10, 9) + "," + 
		    			query2time * 1.0/(long) Math.pow(10, 9) +"," + (query1time + query2time )* 1.0 /(long) Math.pow(10, 9)
		    			+ "," + combinedtime * 1.0 /(long) Math.pow(10, 9));
		    }
		    
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
	}

}
