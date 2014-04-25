package microbenchmarks;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import utils.CommonOperations;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MultipleAggregate {
	private int working_mem;
	private String table;
	private Connection connection;
	private String selectedDimAttribute;
	private int nMeasures;
	
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
	
	public MultipleAggregate(String table, int working_mem, String selectedDimAttribute, int nMeasures, 
			String dbType, String dbAddress, String dbUser, String dbPassword) {
		this.table = table;
		this.working_mem = working_mem;
		this.connection = this.getConnection(dbType, dbAddress, dbUser, dbPassword);
		this.selectedDimAttribute = selectedDimAttribute;
		this.nMeasures = nMeasures;
	}
	
	public void runMultipleAggregateTest() throws SQLException {
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
		Statement stmt = connection.createStatement();
		ResultSet rs = null;
		stmt.execute("set work_mem=" + working_mem + ";");
		for (int i = 1; i <= nMeasures; i=i+2) {
			List<String> aggList = Lists.newArrayList();
			for (int j = 1; j <= i; j++) {
				aggList.add("count(measure"+j + ")");
			}
			String sqlQuery = "SELECT " + selectedDimAttribute + "," + Joiner.on(", ").join(aggList) 
					+ " FROM " + table + " GROUP BY " + selectedDimAttribute;
			long combinedTime = System.nanoTime();
			rs = stmt.executeQuery(sqlQuery);
			combinedTime = System.nanoTime() - combinedTime;
			
			long individualTime = System.nanoTime();
			for (String singleAttr : aggList) {
				sqlQuery = "SELECT " + selectedDimAttribute + "," + singleAttr + " FROM " + table + 
						" GROUP BY " + selectedDimAttribute;
				rs = stmt.executeQuery(sqlQuery);
			}
			individualTime = System.nanoTime() - individualTime;
			System.out.println(table + "," + working_mem + "," + i + "," + combinedTime + "," + individualTime);
		}
	}
}
