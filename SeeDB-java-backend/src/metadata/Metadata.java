package metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.postgresql.util.PSQLException;

import utils.CommonOperations;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Metadata {
	private int working_mem;
	private String table;
	private Connection connection;
	
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
	
	public Metadata(String table, int working_mem, String dbType, String dbAddress, String dbUser, String dbPassword) {
		this.table = table;
		this.working_mem = working_mem;
		this.connection = this.getConnection(dbType, dbAddress, dbUser, dbPassword);
	}
	
public Map<String, String> getColumnAttributes() throws SQLException {
		
		Map<String, String> columnAttributes = new HashMap<String, String>();

		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
		DatabaseMetaData dbmd = null;
		ResultSet rs = null;
		try {
			dbmd = connection.getMetaData();
			rs = dbmd.getColumns(null, null, table, null);

		} catch (SQLException e) {
			System.out.println("Error in executing metadata query");
			e.printStackTrace();
			return columnAttributes;
		}
		
		List<String> dimensionAttributes = Lists.newArrayList();
		List<String> measureAttributes = Lists.newArrayList();
		
		while (rs.next()) {
			String name = rs.getString("COLUMN_NAME");
			String type = rs.getString("TYPE_NAME");
			columnAttributes.put(name, type);
		}

		return columnAttributes;
	}

	public Integer getNumRows() throws SQLException {
		
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
			
		String sqlQuery = "SELECT count(*) FROM " + table;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
		    rs = stmt.executeQuery(sqlQuery);
	
		    rs.next();
		 	return rs.getInt(1);
		    
		    
		} catch (PSQLException e) {
			System.out.println(e.getMessage());
		}
		
		return null;
	}

	public Float getVariance(String columnName) throws SQLException {
		
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
			
		String sqlQuery = "SELECT var_pop(" + columnName + ") FROM " + table;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
		    rs = stmt.executeQuery(sqlQuery);
	
		    rs.next();
		 	return rs.getFloat(1);
		    
		    
		} catch (PSQLException e) {
			if (e.getMessage().contains("var_pop") && e.getMessage().contains("does not exist")) {
			 	return null;
			} else {
				System.out.println(e.getMessage());
			}
		}
		
		return null;
	}
	
	
	public Integer getNumDistinct(String columnName) throws SQLException {
		
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
					
		String sqlQuery = "SELECT count(distinct(" + columnName + ")) FROM " + table;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
		    rs = stmt.executeQuery(sqlQuery);

		    rs.next();
		    return rs.getInt(1);
		    
		    
		} catch (PSQLException e) {
			if (e.getMessage().contains("variance") && e.getMessage().contains("does not exist")) {
				return null;
			} else {
				System.out.println(e.getMessage());
			}
		}
		
		return null;
	}
	
	public String getType(String columnName, String columnType, Integer numRows, Integer numDistinct ) throws SQLException {
	 
		if (columnType.contains("char") || columnType.contains("bit")) {
			if (numDistinct < 20 || numDistinct * 10 < numRows) {
				return "Categorical";
			} else {
				return "String"; //Geographic?
			}
		} else if (columnType.contains("time")) {
			return "Time";
		} else if (columnType.contains("date")) {
			return "Date";
		}
		//Ordinal?
		return "Numeric";
	}	

}
