package api;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import optimizer.Optimizer;
import views.View;

import com.google.common.collect.Lists;

import common.Attribute;
import common.DBSettings;
import common.DifferenceQuery;
import common.ExperimentalSettings.ComparisonType;
import common.ExperimentalSettings.DifferenceOperators;
import common.ConnectionPool;
import common.InputQuery;
import common.InputTablesMetadata;
import common.QueryExecutor;
import common.ExperimentalSettings;
import common.QueryParser;
import db_wrappers.DBConnection;
import difference_operators.AggregateGroupByDifferenceOperator;
import difference_operators.CardinalityDifferenceOperator;
import difference_operators.DifferenceOperator;
import difference_operators.RowSampleDifferenceOperator;

/**
 * This class defines all the functions that the front end can use to 
 * communicate with the backend
 * 
 * @author manasi
 */
public class SeeDB {
	public static enum Datasets { DATASET1, DATASET2 };
	private DBConnection connection;
	private InputQuery[] inputQueries;
	private int numDatasets;
	private ExperimentalSettings settings;
	private HashMap<ExperimentalSettings.DifferenceOperators, 
		DifferenceOperator> supportedOperators = new HashMap<
			ExperimentalSettings.DifferenceOperators, 
			DifferenceOperator>();
	private ConnectionPool pool;
	
	public SeeDB() {
		connection = new DBConnection();
		inputQueries = new InputQuery[]{null, null};
		numDatasets = 0;
		supportedOperators.put(DifferenceOperators.CARDINALITY, new CardinalityDifferenceOperator());
		supportedOperators.put(DifferenceOperators.AGGREGATE, new AggregateGroupByDifferenceOperator());
		supportedOperators.put(DifferenceOperators.DATA_SAMPLE, new RowSampleDifferenceOperator());
	}
	
	public DBConnection getConnection() {
		return connection;
	}
	
	public int getNumDatasets() {
		return numDatasets;
	}
	
	public ExperimentalSettings getSettings() {
		return settings;
	}
	
	public InputQuery[] getInputQueries() {
		return inputQueries;
	}
	
	/**
	 * Connect to a database. Each dataset can connect to only one database
	 * @param database String with which to connect to the database
	 * @param databaseType Type of database to connect to
	 * @param username username for database
	 * @param password password for database
	 * @param the dataset for which we are connecting to the database.
	 *         possible values: DATASET1, DATASET 2
	 * @return true if connection is successful    	
	 */
	public boolean connectToDatabase(String database, String databaseType, 
			String username, String password) {
		return connection.connectToDatabase(database, databaseType, 
				username, password);
	}
	
	/**
	 * connectToDatabase with default settings
	 * @param datasetNum
	 * @return
	 */
	public boolean connectToDatabase() {
		return connection.connectToDatabase(DBSettings.getDefault());
	}

	public boolean initializeManual(String query) throws Exception {
		if (query == null) return false;
		if (connection.hasConnection()) {
			// populate connection from settings
			if (connection.connectToDatabase(DBSettings.getDefault()))
				return false;
		}
		inputQueries[0] = QueryParser.parse(query, 
				connection.getDatabaseName());
		this.numDatasets = 1;
		this.settings = ExperimentalSettings.getDefault();
		this.settings.comparisonType = ComparisonType.MANUAL_VIEW;
		this.settings.differenceOperators = Lists.newArrayList();
		return true;
	}
	
	/**
	 * Initialize SeeDB engine with the two datasets and optionally experimental settings. 
	 * Also sanitize the input
	 * @param dataset1
	 * @param dataset2
	 * @param settings
	 * @return true if init is successful
	 * @throws Exception 
	 */
	public boolean initialize(String query1, String query2, 
			ExperimentalSettings settings) throws Exception {
		
		// first process query1, this cannot be empty
		if (query1 == null) {
			throw new Exception("query1 cannot be null");
		}
		if (connection.hasConnection()) {
			// populate connection from settings
			if (!connection.connectToDatabase(DBSettings.getDefault()))
				return false;
		}
		inputQueries[0] = QueryParser.parse(query1, 
				connection.getDatabaseName());

		// if dataset2 is not empty
		if (query2 != null) {
			this.numDatasets = 2;
			settings.comparisonType = ExperimentalSettings.ComparisonType.TWO_DATASETS;
			inputQueries[1] = QueryParser.parse(query2, connection.getDatabaseName());
		}
		// there is only 1 dataset
		else {
			this.numDatasets = 1;
				if (settings.comparisonType == ExperimentalSettings.ComparisonType.TWO_DATASETS)
				settings.comparisonType = 
					ExperimentalSettings.ComparisonType.ONE_DATASET_FULL;
			if (settings.comparisonType == ExperimentalSettings.ComparisonType.ONE_DATASET_FULL) {
				inputQueries[1] = InputQuery.deepCopy(inputQueries[0]);
				inputQueries[1].whereClause = "";
				// TODO: update raw query for input query 1
			}
			else {
				inputQueries[1] = InputQuery.deepCopy(inputQueries[0]);
				inputQueries[1].whereClause = "NOT(" + inputQueries[0].whereClause + ")";
			}
		}
		this.settings = settings;
		if (this.settings.useParallelExecution) {
			this.pool = new ConnectionPool(settings.maxDBConnections, connection.database, connection.databaseType,
					connection.username, connection.password);
		}
		return true;
	}
	
	/**
	 * Initialize seeDB with default setings
	 * @param dataset1
	 * @param dataset2
	 * @return true if init is successful
	 * @throws Exception 
	 */
	public boolean initialize(String query1, String query2) throws Exception {
		return this.initialize(query1, query2, ExperimentalSettings.getDefault());
	}
	
	/**
	 * Compute the metadata for both the queries
	 * TODO: to update with data needed at frontend
	 * @return
	 */
	public InputTablesMetadata[] getMetadata(List<String> tables1,
			List<String> tables2, int numDatasets) {
		InputTablesMetadata[] queryMetadatas = new InputTablesMetadata[] {null, null};
		queryMetadatas[0] = new InputTablesMetadata(tables1, this.connection);
		if (numDatasets == 2) {
			queryMetadatas[1] = new InputTablesMetadata(tables2, this.connection);
			// TODO: need to ensure that same tables are being queried
		}
		return queryMetadatas;
	}
	
	public InputTablesMetadata[] getMetadata() {
		return this.getMetadata(inputQueries[0].tables, inputQueries[1].tables, this.numDatasets);
	}
	
	public List<DifferenceOperator> getDifferenceOperators() {
		List<DifferenceOperator> ops = Lists.newArrayList();
		for (ExperimentalSettings.DifferenceOperators op : 
			settings.differenceOperators) {
			if (op == ExperimentalSettings.DifferenceOperators.ALL) {	
				ops.addAll(supportedOperators.values());
				break;
			}
			ops.add(supportedOperators.get(op));
		}
		return ops;
	}

	/**
	 * Computes the differences between the two datasets and returns the 
	 * serialized results of calling each of the difference operators 
	 * registered with the system
	 * @return List of serialized difference results
	 */
	public List<View> computeDifference() {
		// compute the attributes that we want to analyze
		InputTablesMetadata[] queryMetadatas = this.getMetadata(inputQueries[0].tables,
				inputQueries[1].tables, this.numDatasets);
		
		// compute queries we want to execute
		List<DifferenceOperator> ops = this.getDifferenceOperators();
		List<DifferenceQuery> queries = Lists.newArrayList();
		for (DifferenceOperator op : ops) {
			queries.addAll(op.getDifferenceQueries(inputQueries, queryMetadatas, 
					numDatasets, settings));
		}
		
		// ask optimizer to optimize queries
		Optimizer optimizer = new Optimizer(settings);
		List<DifferenceQuery> optimizedQueries = 
				optimizer.optimizeQueries(queries);
		
		List<View> views = null;
		QueryExecutor qe = new QueryExecutor(pool);
		try {
			views = qe.execute(optimizedQueries, queries, connection, numDatasets);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		System.out.println(views);
		
		return views;
	}	

	public View computeManualView(String x_axis, String y_axis, String aggFunction) {
		DifferenceQuery dq = new DifferenceQuery(null, this.inputQueries);
		dq.groupByAttributes.add(new Attribute(x_axis)); // group by attribute
		List<String> aggFuncs = Lists.newArrayList();
		aggFuncs.add(aggFunction);
		dq.addAggregateAttribute(new Attribute(y_axis), aggFuncs);
		List<DifferenceQuery> derivedFrom = Lists.newArrayList();
		derivedFrom.add(dq);
		dq.derivedFrom = derivedFrom;
		View view = null;
		QueryExecutor qe = new QueryExecutor(null);
		try {
			view = qe.executeSingle(dq, connection, numDatasets);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return view;
	}
}
