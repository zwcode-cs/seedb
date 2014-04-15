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

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
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
	
public List<String> getColumnNames() throws SQLException {
		
		List<String> columnNames = Lists.newArrayList();

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
			return columnNames;
		}
		
		List<String> dimensionAttributes = Lists.newArrayList();
		HashMap<String, Integer> dimensionAttributeDistinctValues = Maps.newHashMap();
		List<String> measureAttributes = Lists.newArrayList();
		
		while (rs.next()) {
			String attribute = rs.getString("COLUMN_NAME");
			if (attribute.startsWith("dim")) {
				dimensionAttributes.add(attribute);
				dimensionAttributeDistinctValues.put(attribute, Integer.parseInt(attribute.split("_")[1])); // col names are dim10_50
			}
			else if (attribute.startsWith("measure")) measureAttributes.add(attribute);
		}
		
		columnNames.addAll(dimensionAttributes);
		columnNames.addAll(measureAttributes);
	
		return columnNames;
	}

	public Float getVariance(String columnName) throws SQLException {
		
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
			
		String sqlQuery = "SELECT variance(" + columnName + ") FROM " + table;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
		    rs = stmt.executeQuery(sqlQuery);
	
		    rs.next();
		 	return rs.getFloat(1);
		    
		    
		} catch (PSQLException e) {
			if (e.getMessage().contains("variance") && e.getMessage().contains("does not exist")) {
			 	return null;
			} else {
				System.out.println(e.getMessage());
			}
		}
		
		return null;
	}
	
	
public Float getNumDistinct(String columnName) throws SQLException {
		
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
		    return rs.getFloat(1);
		    
		    
		} catch (PSQLException e) {
			if (e.getMessage().contains("variance") && e.getMessage().contains("does not exist")) {
				return null;
			} else {
				System.out.println(e.getMessage());
			}
		}
		
		return null;
	}
	
	public String getAttribute(String columnName) throws SQLException {
	
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
		DatabaseMetaData dbmd = null;
		ResultSet rs = null;
	
	
				
		String sqlQuery = "SELECT " + columnName + " FROM " + table;
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
		    rs = stmt.executeQuery(sqlQuery);
		    Set<String> uniqueValues = new HashSet<String>();
		    int rowCount = 0;
		    
		    int isTime = 0;
		    int isGeographic = 0; //XXX:not done
		    int isNumeric = 0;
		    boolean isCategorical = false;
		    int isOrdinal = 0; //XXX: not done

		    while (rs.next()) {

		    	uniqueValues.add(rs.getString(1));

		    	try {
		    		if (rs.getTimestamp(1) != null)  {
		    			isTime++;
			    	}
		    	} catch (PSQLException e) {
		    	}
		    	
		    	try {
		    		if (rs.getBigDecimal(1) != null)  {
		    			isNumeric++;
		    		}			    		
		    	} catch (PSQLException e) {
		    	}
		    	
		    		
	    		System.out.println(rs.getString(1));
	    		String shit = new GeocodeImplementation().getJSONByGoogle(rs.getString(1));
	    		System.out.println(shit);

		    	
		    	rowCount++;
		    }
		    
		    if (uniqueValues.size() < 20 || uniqueValues.size() < 0.1 * rowCount) {
		    	isCategorical = true;
		    }
		    
		    return null;
		    
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
	}	

}
