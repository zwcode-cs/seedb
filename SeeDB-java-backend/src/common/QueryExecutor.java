package common;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.DifferenceOperators;
import utils.Constants.AggregateFunctions;
import views.AggregateGroupByView;
import views.AggregateView;
import views.CardinalityView;
import views.RowSampleView;
import views.View;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


import db_wrappers.DBConnection;

public class QueryExecutor {
	private HashMap<DifferenceQuery, AggregateView> aggregateViewMap; 	// keeps track of results for each view
																		// doesn't need locking because each thread accesses different keys
	private ConnectionPool pool;										// pool used by phase1 of the executor
	private ConnectionPool secondaryPool;								// pool used by phase2 of the executor
	private ExperimentalSettings settings;
	private List<Thread> threads = Lists.newArrayList();				// keep track of threads spawned by executor
	private int nqueries = 0;											// tracking info
	private File logFile;												// file to log to
	private long tempTableCreationTime = 0;								// time to create temp table
	private long dbmsComparisonViewExecutionTime = 0;					// DBMS comparison view execution
	private long dbmsTargetViewExecutionTime = 0;						// DBMS target view execution
	private long clientComparisonViewExecutionTime = 0;					// Client comparison view execution
	private long clientTargetViewExecutionTime = 0;						// Client target view execution
	
	/**
	 * constructor
	 * @param pool
	 * @param settings
	 * @param logFile
	 */
	public QueryExecutor(ConnectionPool pool, ExperimentalSettings settings, File logFile) {
		this.pool = pool;
		this.settings = settings;
		this.logFile = logFile;
	}
	
	/**
	 * 
	 * @param optimizedQueries
	 * @param queries
	 * @param connection
	 * @param numDatasets
	 * @return
	 * @throws SQLException
	 */
	public List<View> execute(List<DifferenceQuery> optimizedQueries,
			List<DifferenceQuery> queries, DBConnection connection, int numDatasets) throws SQLException {
		long start = System.currentTimeMillis();
		List<View> views = Lists.newArrayList();
		for (DifferenceQuery optQuery : optimizedQueries) {
			if (optQuery.op == ExperimentalSettings.DifferenceOperators.DATA_SAMPLE) {
				views.add(executeRowSampleDifferenceQuery(optQuery, connection));
			}
			else if (optQuery.op == DifferenceOperators.CARDINALITY ||
					optQuery.op == DifferenceOperators.AGGREGATE) {
				if (aggregateViewMap == null) {
					aggregateViewMap = Maps.newHashMap();
					// create dummy views for all the views we care about
					for (DifferenceQuery query : queries) {
						if (query.op == DifferenceOperators.CARDINALITY) {
							aggregateViewMap.put(query, new CardinalityView(query));
						}
						if (query.op == DifferenceOperators.AGGREGATE) {
							aggregateViewMap.put(query, new AggregateGroupByView(query));
						}		
					}
				}
				executeAggregateDifferenceQuery(optQuery, connection);
			}
		}
		
		// wait for any spawned threads to finish
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				System.out.println("Thread join interrupted");
				e.printStackTrace();
			}
		}
		
		// clean up
		if (secondaryPool != null) {
			secondaryPool.closeAllConnections();
		}
		
		// populate results
		if (aggregateViewMap != null) {
			views.addAll(aggregateViewMap.values());
		}
		
		// log
		Utils.writeToFile(logFile, "TempTableCreation: " + (tempTableCreationTime/nqueries));
		Utils.writeToFile(logFile, "DBMSTargetViewExecutionTime: " + (dbmsTargetViewExecutionTime/nqueries));
		Utils.writeToFile(logFile, "DBMSComparisonViewExecutionTime: " + (dbmsComparisonViewExecutionTime/nqueries));
		Utils.writeToFile(logFile, "ClientTargetViewExecutionTime: " + (clientTargetViewExecutionTime/nqueries));
		Utils.writeToFile(logFile, "ClientComparisonViewExecutionTime: " + (clientComparisonViewExecutionTime/nqueries));
		Utils.writeToFile(logFile, "Executor: " + (System.currentTimeMillis() - start));
		Utils.writeToFile(logFile, "nQueries: " + nqueries);
		return views;
	}
	
	/**
	 * master function for executing queries. has various paths depending on optimizations
	 * @param optQuery
	 * @param connection
	 * @throws SQLException
	 */
	public void executeAggregateDifferenceQuery(
			DifferenceQuery optQuery, DBConnection connection) throws SQLException {
		// if intermediate results should be written to temp tables
		if (settings.useTempTables) {
			if (!settings.mergeQueries) {
				System.out.println("can use temp tables only with merged queries.");
				return;
			}
			executeAggregateDifferenceQueryWithTempTables(optQuery, connection);
			return;
		}
		
		// each query is derived from itself
		List<String> queries = optQuery.getSQLQuery();
		if (optQuery.derivedFrom == null) {
			optQuery.derivedFrom = Lists.newArrayList();
			optQuery.derivedFrom.add(optQuery);
		} else if (optQuery.derivedFrom.isEmpty()) {
			optQuery.derivedFrom.add(optQuery);
		}
		
		if (!settings.useParallelExecution) {
			if (optQuery.mergedQueries) { 
				executeQuery(optQuery, queries.get(0), connection, -1);
			} else {
				executeQuery(optQuery, queries.get(0), connection, 1);
				executeQuery(optQuery, queries.get(1), connection, 2);
			}
		} else {
			// parallel query execution
			if (optQuery.mergedQueries) { 
				Thread t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(0), -1));
				threads.add(t);
				t.start();
			} else {
				Thread t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(0), 1));
				threads.add(t);
				t.start();
				t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(1), 2));
				threads.add(t);
				t.start();
			}
		}
	}
	
	/**
	 * execute view queries with temp tables
	 * @param optQuery
	 * @param connection
	 * @throws SQLException
	 */
	public void executeAggregateDifferenceQueryWithTempTables(
			DifferenceQuery optQuery, DBConnection connection) throws SQLException {
		if (settings.useParallelExecution) {
			// create secondary pool
			if (secondaryPool == null) {
				secondaryPool = new ConnectionPool(settings.maxDBConnections, connection.database, connection.databaseType, connection.username, connection.password);
			}
			Thread t = new Thread(new ExecuteParallelQueryWithTempTables(optQuery));
			threads.add(t);
			t.start();
		} else {
			// execute query that puts results in temp table
			List<String> optSQLQuery = optQuery.getSQLQuery(true);
			long start = System.currentTimeMillis();
			//System.out.println(optSQLQuery.get(0));
			connection.executeQueryWithoutResult(optSQLQuery.get(0));
			tempTableCreationTime += System.currentTimeMillis() - start;
			
			// process the data from the temp table
			List<String> queries = optQuery.getSQLForParentQueries(true);
			for (int i = 0; i < optQuery.derivedFrom.size();i++) {
				executeQuery(optQuery.derivedFrom.get(i), queries.get(i*2), connection, 1);
				executeQuery(optQuery.derivedFrom.get(i), queries.get(i*2+1), connection, 2);
			}
			connection.executeQueryWithoutResult("DROP TABLE IF EXISTS " + "seedb_tmp_table_" + optQuery.seqNum);
		}
	}

	/**
	 * helper function to execute any query on the database
	 * @param optQuery
	 * @param query
	 * @param con
	 * @param group
	 * @throws SQLException
	 */
	public void executeQuery(DifferenceQuery optQuery, String query, 
			DBConnection con, int group) throws SQLException {
		nqueries++;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		long start = System.currentTimeMillis();
		rs = con.executeQuery(query);
		if (group < 2) {
			dbmsTargetViewExecutionTime += System.currentTimeMillis() - start;	
		} else {
			dbmsComparisonViewExecutionTime += System.currentTimeMillis() - start;	
		}
		
		// get metadata for query results
		start = System.currentTimeMillis();
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		List<String> columnNames = Lists.newArrayList();
		HashMap<String, Object> row = Maps.newHashMap();
		
		// The column count starts from 1
		for (int i = 1; i < columnCount + 1; i++ ) {
		  columnNames.add(rsmd.getColumnName(i));
		}
		
		// get actual query results
		while (rs.next()) {
			for (int i = 1; i < columnCount + 1; i++ ) {
				String columnName = columnNames.get(i-1).toLowerCase();
				if (columnName.endsWith("count") || columnName.endsWith("sum")) {
					row.put(columnName, rs.getDouble(i));
				} else {
					row.put(columnName, rs.getObject(i));
				}
			}

			for (DifferenceQuery dq : optQuery.derivedFrom) { // for each query whose result this query contributes to
				AggregateView view = aggregateViewMap.get(dq);
				List<String> gbAttrs = 
						DifferenceQuery.getAttributeNames(dq.groupByAttributes);
				List<String> aggAttrs = 
						DifferenceQuery.getColumnNamesForAggregateAttributes(
								dq.aggregateAttributes, dq.aggregateFunctions);
				String groupBy = null;
				if (gbAttrs.isEmpty()) {
					groupBy = "NONE";				
				}
				else {
					List<String> gbValues = Lists.newArrayList();
					for (String attr : gbAttrs) {
						gbValues.add(row.get(attr).toString());	
					}
					groupBy = Joiner.on("__").join(gbValues);
				}
				
				for (String attr : aggAttrs) {
					attr = attr.toLowerCase();
					AggregateFunctions func = attr.endsWith("count") ? AggregateFunctions.COUNT : AggregateFunctions.SUM;
					if (group == -1) {
						if (row.get("seedb_group_1").equals(1) || row.get("seedb_group_1").equals(new Long(1))) {
							view.addAggregateValue(groupBy, func, row.get(attr), 1);	
						}
						if (row.get("seedb_group_2").equals(1) || row.get("seedb_group_2").equals(new Long(1))) {
							view.addAggregateValue(groupBy, func, row.get(attr), 2);	
						}
					}
					else {
						view.addAggregateValue(groupBy, func, row.get(attr), group);
					}
				}
			}
		}
		if (group < 2) {
			clientTargetViewExecutionTime += System.currentTimeMillis() - start;
		} else {
			clientComparisonViewExecutionTime += System.currentTimeMillis() - start;
		}
	}
	
	// class to execute queries on parallel threads
	private class ExecuteParallelQuery implements Runnable {
		DifferenceQuery optQuery;
		String query; 
		int group;
		
		public ExecuteParallelQuery(DifferenceQuery optQuery, String query, 
				int group) {
			this.optQuery = optQuery;
			this.query = query;
			this.group = group;
		}

		@Override
		public void run() {
			Connection c;
			try {
				c = pool.getAvailableConnection();
			} catch (InterruptedException e) {
				c = null;
				e.printStackTrace();
			}
			if (c==null) {
				System.out.println("Cannot get connection to database");
				return;
			}
			DBConnection con = new DBConnection(c);
			try {
				executeQuery(optQuery, query, con, group);
				pool.returnConnectionToPool(c);
			} catch (SQLException e) {
				System.out.println("Error executing query: " + query);
				e.printStackTrace();
				return;
			}
		}
	}
	
	// class to execute queries on parallel threads
	private class ExecuteParallelQueryWithTempTables implements Runnable {
		DifferenceQuery optQuery;							// query to execute
		List<Thread> localThreads = Lists.newArrayList();	// threads spawned
		
		public ExecuteParallelQueryWithTempTables(DifferenceQuery optQuery) {
			this.optQuery = optQuery;
		}

		@Override
		public void run() {
			List<String> optSQLQuery = optQuery.getSQLQuery(true);
			Connection c;
			try {
				c = secondaryPool.getAvailableConnection();
			} catch (InterruptedException e) {
				c = null;
				e.printStackTrace();
			}
			if (c == null) {
				System.out.println("Cannot get connection to database");
				return;
			}
			
			DBConnection con = new DBConnection(c);
			long start = System.currentTimeMillis();
			con.executeQueryWithoutResult(optSQLQuery.get(0));
			//System.out.println(optSQLQuery.get(0));
			tempTableCreationTime += System.currentTimeMillis() - start;
			
			// process the data from the temp table
			List<String> queries = optQuery.getSQLForParentQueries(true);
			for (int i = 0; i < optQuery.derivedFrom.size();i++) {
				Thread t = new Thread(new ExecuteParallelQuery(optQuery.derivedFrom.get(i), queries.get(i*2), 1));
				localThreads.add(t);
				t.start();
				t = new Thread(new ExecuteParallelQuery(optQuery.derivedFrom.get(i), queries.get(i*2+1), 2));
				localThreads.add(t);
				t.start();
			}
			
			for (Thread t : localThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			con.executeQueryWithoutResult("DROP TABLE IF EXISTS " + optQuery.getTempTableName());
			secondaryPool.returnConnectionToPool(c);	
		}	
	}
	
	// execute row sample query
	public View executeRowSampleDifferenceQuery(
			DifferenceQuery optQuery, DBConnection connection) 
					throws SQLException {
		RowSampleView view = new RowSampleView();
		List<String> queries = optQuery.getSQLQuery();
		List<String> limitedQueries = Lists.newArrayList();
		
		for (String query : queries) {
			limitedQueries.add(query + " LIMIT 50"); // TODO this is weird
		}
		ResultSet rs = connection.executeQuery(
				queries.get(0));

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		// The column count starts from 1
		for (int i = 1; i < columnCount + 1; i++ ) {
		  view.columnNames.add(rsmd.getColumnName(i));
		}
		
		while (rs.next()) {
			List<String> row = Lists.newArrayList();
			for (int i = 1; i < columnCount + 1; i++ ) {
				row.add(rs.getString(i));
			}
			view.rows1.add(row);
		}

		rs = connection.executeQuery(
				queries.get(1));
		while (rs.next()) {
			List<String> row = Lists.newArrayList();
			for (int i = 1; i < columnCount + 1; i++ ) {
				row.add(rs.getString(i));
			}
			view.rows2.add(row);
		}
		return view;
	}
}
