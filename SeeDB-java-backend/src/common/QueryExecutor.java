package common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import views.AggregateGroupByView;
import views.AggregateView;
import views.CardinalityView;
import views.RowSampleView;
import views.View;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.ExperimentalSettings.DifferenceOperators;

import db_wrappers.DBConnection;

public class QueryExecutor {
	private HashMap<DifferenceQuery, AggregateView> aggregateViewMap; // doesn't need locking because each thread accesses different keys
	private ConnectionPool pool;

	public QueryExecutor(ConnectionPool pool) {
		this.pool = pool;
	}
	
	public List<View> execute(List<DifferenceQuery> optimizedQueries,
			List<DifferenceQuery> queries, DBConnection connection, int numDatasets) throws SQLException {
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
		if (aggregateViewMap != null) {
			views.addAll(aggregateViewMap.values());
		}
		return views;
	}
	
	public void executeAggregateDifferenceQuery(
			DifferenceQuery optQuery, DBConnection connection) throws SQLException {
		List<String> queries = optQuery.getSQLQuery();
		if (pool == null) {
			if (optQuery.mergedQueries) { 
				executeQuery(optQuery, queries.get(0), connection, -1);
			} else {
				executeQuery(optQuery, queries.get(0), connection, 1);
				executeQuery(optQuery, queries.get(1), connection, 2);
			}
		} else {
			List<Thread> threads = Lists.newArrayList();
			if (optQuery.mergedQueries) { 
				Thread t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(0), -1));
				threads.add(t);
				t.start();
			} else {
				Thread t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(0), 1));
				threads.add(t);
				t.start();
				t = new Thread(new ExecuteParallelQuery(optQuery, queries.get(0), 2));
				threads.add(t);
				t.start();
			}
			for (Thread t : threads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					System.out.println("Thread join interrupted");
					e.printStackTrace();
				}
			}
			pool.closeAllConnections();
		}
	}

	public void executeQuery(DifferenceQuery optQuery, String query, 
			DBConnection con, int group) throws SQLException {
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		rs = con.executeQuery(query);
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		List<String> columnNames = Lists.newArrayList();
		HashMap<String, Object> row = Maps.newHashMap();
		
		// The column count starts from 1
		for (int i = 1; i < columnCount + 1; i++ ) {
		  columnNames.add(rsmd.getColumnName(i));
		}
		
		while (rs.next()) {
			for (int i = 1; i < columnCount + 1; i++ ) {
				String columnName = columnNames.get(i-1);
				if (columnName.endsWith("count")) {
					row.put(columnName, rs.getDouble(i));
				}
				else if (columnName.endsWith("sum")) {
					row.put(columnName, rs.getDouble(i));
				}
				else {
					row.put(columnName, rs.getObject(i));
				}
			}
			for (DifferenceQuery dq : optQuery.derivedFrom) {
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
						gbValues.add((String) row.get(attr));	
					}
					groupBy = Joiner.on("__").join(gbValues);
				}
				
				for (String attr : aggAttrs) {
					attr = attr.toLowerCase();
					if (group == -1) {
						if ((Integer) row.get("seedb_group_1") == 1) {
							view.addAggregateValue(groupBy, attr, row.get(attr), 1);	
						}
						if ((Integer) row.get("seedb_group_2") == 1) {
							view.addAggregateValue(groupBy, attr, row.get(attr), 2);	
						}
					}
					else {
						view.addAggregateValue(groupBy, attr, row.get(attr), group);
					}
				}
			}
		}		
	}

	public View executeRowSampleDifferenceQuery(
			DifferenceQuery optQuery, DBConnection connection) 
					throws SQLException {
		RowSampleView view = new RowSampleView();
		List<String> queries = optQuery.getSQLQuery();
		List<String> limitedQueries = Lists.newArrayList();
		
		for (String query : queries) {
			limitedQueries.add(query + " LIMIT 50");
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

	public View executeSingle(DifferenceQuery dq, DBConnection connection,
			int numDatasets) throws SQLException {
		aggregateViewMap = Maps.newHashMap();
		AggregateView v = new AggregateGroupByView(dq);
		aggregateViewMap.put(dq, v);
		String query = dq.getSQLQueryHelper(false, dq.inputQueries[0], null);
		executeQuery(dq, query, connection, 1);
		return v;
	}
	
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
			} catch (SQLException e) {
				System.out.println("Error executing query: " + query);
				e.printStackTrace();
				return;
			}
		}
		
	}
	
}
