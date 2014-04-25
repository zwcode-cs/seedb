package microbenchmarks;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import utils.CommonOperations;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MultipleGroupBy {
	private int working_mem;
	private String table;
	private Connection connection;
	private String[] attrs;
	private long ngroups;
	
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
	
	public MultipleGroupBy(String table, int working_mem, String[] attrs, long ngroups,
			String dbType, String dbAddress, String dbUser, String dbPassword) {
		this.table = table;
		this.working_mem = working_mem;
		this.attrs = attrs;
		this.connection = this.getConnection(dbType, dbAddress, dbUser, dbPassword);
		this.ngroups = ngroups;
	}
	
	private String join(String[] attrs) {
		String ret =attrs[0];
		for (int i = 1; i < attrs.length; i++) {
			ret += "," + attrs[i];
		}
		return ret;
	}
	
	public void runMultipleGroupByTest() throws SQLException {
		if (connection == null) {
			System.out.println("Connection null. Quit");
		}
		
		Statement stmt = connection.createStatement();
		ResultSet rs = null;
		stmt.execute("set work_mem=" + working_mem + ";");
		
		// combined query
		long combinedTime = System.nanoTime();
		String sqlQuery = "SELECT " + join(attrs) + ", COUNT(measure1) FROM s_1 GROUP BY " + join(attrs);
		//System.out.println(sqlQuery);
		rs = stmt.executeQuery(sqlQuery);
		combinedTime = System.nanoTime() - combinedTime;
		
		// individual queries
		long individualTime = System.nanoTime();
		for (String attr : attrs) {
			sqlQuery = "SELECT " + attr + ", COUNT(measure1) FROM s_1 GROUP BY " + attr;
			//System.out.println(sqlQuery);
			rs = stmt.executeQuery(sqlQuery);
		}
		individualTime = System.nanoTime() - individualTime;
		System.out.println(table + "," + working_mem + "," + ngroups + "," + combinedTime + "," + individualTime);
	}
	

}
