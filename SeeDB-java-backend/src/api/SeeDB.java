package api;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import output.OutputView;

import optimizer.Optimizer;
import views.View;

import com.google.common.collect.Lists;

import common.DBSettings;
import common.DifferenceQuery;
import common.ExperimentalSettings.DifferenceOperators;
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
	private DBConnection[] connections;
	private InputQuery[] inputQueries;
	private int numDatasets;
	private ExperimentalSettings settings;
	private HashMap<ExperimentalSettings.DifferenceOperators, 
		DifferenceOperator> supportedOperators = new HashMap<
			ExperimentalSettings.DifferenceOperators, 
			DifferenceOperator>();
	
	public SeeDB() {
		connections = new DBConnection[]{new DBConnection(), 
				new DBConnection()};
		inputQueries = new InputQuery[]{null, null};
		numDatasets = 0;
		supportedOperators.put(DifferenceOperators.CARDINALITY, new CardinalityDifferenceOperator());
		supportedOperators.put(DifferenceOperators.AGGREGATE, new AggregateGroupByDifferenceOperator());
		supportedOperators.put(DifferenceOperators.DATA_SAMPLE, new RowSampleDifferenceOperator());
	}
	
	public DBConnection[] getConnections() {
		return connections;
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
			String username, String password, int datasetNum) {
		if (!(datasetNum > 0 && datasetNum <= Datasets.values().length))
			return false;
		return connections[datasetNum].connectToDatabase(database, databaseType, 
				username, password);
	}
	/**
	 * connectToDatabase with default settings
	 * @param datasetNum
	 * @return
	 */
	public boolean connectToDatabase(int datasetNum) {
		return connections[datasetNum].connectToDatabase(
				DBSettings.getDefault());
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
		if (query1 == null)
			throw new Exception("query1 cannot be null");
		if (!connections[0].hasConnection()) {
			// populate connection from settings
			if (!connections[0].connectToDatabase(DBSettings.getDefault()))
				return false;
		}
		inputQueries[0] = QueryParser.parse(query1, 
				connections[0].getDatabaseName());

		// if dataset2 is not empty
		if (query2 != null) {
			if (!connections[1].hasConnection()) {
				// populate connection from settings
				if (!connections[1].connectToDatabase(DBSettings.getDefault()))
					return false;
			}
			this.numDatasets = 2;
			settings.comparisonType = ExperimentalSettings.ComparisonType.TWO_DATASETS;
			inputQueries[1] = QueryParser.parse(query2, 
					connections[1].getDatabaseName());
		}
		// there is only 1 dataset
		else {
			this.numDatasets = 1;
			connections[1] = connections[0]; // share the connection
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
	 * @return
	 */
	public InputTablesMetadata[] getMetadata() {
		InputTablesMetadata[] queryMetadatas = new InputTablesMetadata[] {null, null};
		queryMetadatas[0] = new InputTablesMetadata(inputQueries[0], connections[0]);
		if (numDatasets == 2) {
			queryMetadatas[1] = new InputTablesMetadata(inputQueries[1], connections[1]);
			InputTablesMetadata.computeIntersection(queryMetadatas[0], 
					queryMetadatas[1]);
		}
		return queryMetadatas;
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
	public List<OutputView> computeDifference() {
		// compute the attributes that we want to analyze
		InputTablesMetadata[] queryMetadatas = this.getMetadata();
		
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
		QueryExecutor qe = new QueryExecutor();
		try {
			views = qe.execute(optimizedQueries, queries, connections, numDatasets);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		// TODO: do something to pick the top k
		List<OutputView> result = Lists.newArrayList(); 
		for (View view : views) {
			result.add(view);
		}
		return result;
	}	
	
}
