package core_v1;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;

import core.DiscriminatingView;
import core.QueryExecutor;

/**
 * This class is the entry point for all SeeDB functionality. Only supported 
 * APIs should be exposed here.
 * 
 * Currently this assumes a postgres backend
 * TODO: make it work with different backends
 * @author manasi
 *
 */
public class SeeDB {
	public static String[] differenceOperators = new String[] {"AggregateBasedDifferenceOperator"};

	/** 
	 * connect to given database. assume username and password is known
	 * TODO: allow specification of database username and password
	 * @param s
	 */
	public static void ConnectToDatabase(String s) {
		QueryExecutor.ConnectToDatabase(s);
	}
	
	/**
	 * returns list of all tables in the database the user is connected to
	 * @return
	 */
	public static List<String> FindAllTablesInDatabase() {
		List<String> tables = Lists.newArrayList();
		String query="select tablename from pg_tables where schemaname='public';";
		ResultSet rs = QueryExecutor.executeQuery(query);
		try {
			while (rs.next()) {
				tables.add(rs.getString(1));
			}
		} catch (SQLException e) {
			return tables;
		}
		return tables;
	}
	
	public static List<DiscriminatingView> computeDifferences(String query1, String query2) {
		List<DiscriminatingView> res = Lists.newArrayList();
		// process query1 and query2
		// TODO: parse queries
		InputQuery input1 = null, input2 = null;
		
		// for all registered difference operators and compute differences
		// TODO: means to register difference operators
		for (String differenceOperator : differenceOperators) {
			try {
				DifferenceOperator diffOp = (DifferenceOperator) Class.forName(differenceOperator).newInstance();
				diffOp.initialize(input1, input2);
				List<DiscriminatingView> views = diffOp.computeDifference();
				res.addAll(views);	
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}	
		}
		return res;
	}
	
}
