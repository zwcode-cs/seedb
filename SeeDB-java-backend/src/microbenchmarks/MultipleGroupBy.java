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
	
	public MultipleGroupBy(String table, int working_mem, String dbType, String dbAddress, String dbUser, String dbPassword) {
		this.table = table;
		this.working_mem = working_mem;
		this.connection = this.getConnection(dbType, dbAddress, dbUser, dbPassword);
	}
	
	public void runMultipleGroupByTest() throws SQLException {
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
			return;
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
		
		int nDims = dimensionAttributes.size();
		for (int i = 1; i <= 5; i++) { //TODO: change to nDims
			List<List<String>> combinations = CommonOperations.getCombinations(i, dimensionAttributes);
			for (List<String> combo : combinations) {
				int ngroups = 1;
				for (String s : combo) {
					ngroups *= dimensionAttributeDistinctValues.get(s);
				}
				
				String sqlQuery = "SELECT COUNT(*) FROM " + table + " GROUP BY " + Joiner.on(",").join(combo);
				Statement stmt = null;
				rs = null;
				try {
					stmt = connection.createStatement();
					/*System.out.println(stmt.execute("show work_mem;"));
					rs = stmt.getResultSet();
					while (rs.next()) {
						System.out.println(rs.getString(1));
					}
					System.out.println(stmt.execute("set work_mem=4000000;"));
					*/
					stmt.execute("set work_mem=" + working_mem + ";");
					long start = System.nanoTime();
				    rs = stmt.executeQuery(sqlQuery);
				    System.out.print(working_mem + "," + table + "," + i + "," + ngroups + ",");
				    System.out.println("Time taken: " + (System.nanoTime() - start) + ", Names:"+Joiner.on("::").join(combo));
				} catch (Exception e) {
					System.out.println("Error in executing query");
				}
			}
		}
		
	}
	

}
