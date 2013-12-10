package core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import utils.DistributionUnit;
import utils.RuntimeSettings;

/**
 * This class does the main processing for SeeDB. It generates queries for discriminating views, executes those queries and
 * picks the top views
 * @author manasi
 *
 */
public class QueryProcessor {
	private String query;
	private String table;
	private String selectPredicate;
	private RuntimeSettings runtimeSettings;
	
	public RuntimeSettings getRuntimeSettings() {
		return runtimeSettings;
	}
	
	public void setRuntimeSettings(RuntimeSettings runtimeSettings) {
		this.runtimeSettings = runtimeSettings;
	}
	
	public String getTable() {
		return table;
	}
	
	public void setTable(String table) {
		this.table = table;
	}
	
	public String getSelectPredicate() {
		return selectPredicate;
	}
	
	public String getDistanceMeasure() {
		return this.runtimeSettings.metric;
	}
	
	public void setDistanceMeasure(String distanceMeasure) {
		this.runtimeSettings.metric = distanceMeasure;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}

	public String getQuery() {
		return query;
	}

	public Metadata getMetadata() {
		// get metadata about table mentioned in the query
		Metadata tableMetadata = new Metadata(table);
		try {
			tableMetadata.updateTableSchema();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return tableMetadata;
	}

	public QueryProcessor() {
		QueryExecutor.Instantiate();
		runtimeSettings = new RuntimeSettings();
	}
	
	/**
	 * Parses the query to find out the predicates in it
	 * //TODO: use JsqlParser. Extremely sensitive to formating!!!!
	 */
	public void ParseQuery() {	
		String[] parts = query.split(" ");
		for (int i = 0; i < parts.length; i++) {
			if (parts[i].trim().toLowerCase().equals("from")) {
				table = parts[i+1].trim(); // assumes that query is properly formatted 
			}
			else if (parts[i].trim().toLowerCase().equals("where")) {
				selectPredicate = parts[i+1].trim(); // assumes that query is properly formatted 
			}
		}
	}
	

	public String AddViewPredicates(String dimensionAttribute, List<String> measureAttributes, boolean queryOnly) {
		// update the query for the aggregate and group by
		int table_idx = query.indexOf(table);
		int end_idx = query.indexOf(";");
		String viewQuery = "select " + dimensionAttribute + ", " ;
		for (Object measureAttribute : measureAttributes) {
			viewQuery += "SUM(" + measureAttribute.toString() + "), ";
		}
		viewQuery = viewQuery.substring(0, viewQuery.length() - 2); // remove extraneous ', '
		viewQuery += " from ";
		if (runtimeSettings.useSampling) {
			viewQuery += "(select * from " + table + " where random() < " 
					+ runtimeSettings.samplePercent + ") as temp ";
			if (queryOnly) {
				viewQuery += query.substring(table_idx + table.length(), end_idx);
			}
		} else {
				viewQuery += (queryOnly ? query.substring(table_idx, end_idx) : table);
		}
		viewQuery += " GROUP BY " + dimensionAttribute + ";";	
		return viewQuery;
	}
	
	public List<DiscriminatingView> Process() {
		long startTime = System.nanoTime();
		// parse query for table and selectPredicate
		ParseQuery();
		
		// get metadata about table mentioned in the query
		Metadata tableMetadata = getMetadata();
		
		ArrayList<String> dimensionAttributes = tableMetadata.getDimensionAttributes();
		ArrayList<String> measureAttributes = tableMetadata.getMeasureAttributes();
		ArrayList<DiscriminatingView> views = new ArrayList<DiscriminatingView>();
		
		for (int dimensionIndex = 0; dimensionIndex < dimensionAttributes.size(); dimensionIndex++) {
			if (dimensionAttributes.get(dimensionIndex).equalsIgnoreCase(selectPredicate)) {
				continue;
			}
			if (runtimeSettings.useMultipleAggregateSingleGroupByOptimization) {
				String aggregateQueryForQuery = AddViewPredicates(dimensionAttributes.get(dimensionIndex), 
						measureAttributes, true);
				String aggregateQueryForDataset = AddViewPredicates(dimensionAttributes.get(dimensionIndex), 
						measureAttributes, false);
				try {
					CreateAndAddDiscriminatingView(aggregateQueryForQuery, aggregateQueryForDataset, 
							measureAttributes.size(), Lists.newArrayList(dimensionAttributes.get(dimensionIndex)), measureAttributes, views);
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}
					
			} else {		
				for (int measureIndex = 0; measureIndex < measureAttributes.size(); measureIndex++) {	
					// add an aggregate and group by: currently on SUM
					String aggregateQueryForQuery = AddViewPredicates(dimensionAttributes.get(dimensionIndex), 
							Lists.newArrayList(measureAttributes.get(measureIndex)), true);
					String aggregateQueryForDataset = AddViewPredicates(dimensionAttributes.get(dimensionIndex), 
							Lists.newArrayList(measureAttributes.get(measureIndex)), false);
					try {
						CreateAndAddDiscriminatingView(aggregateQueryForQuery, aggregateQueryForDataset, 1, 
								Lists.newArrayList(dimensionAttributes.get(dimensionIndex)), Lists.newArrayList(measureAttributes.get(measureIndex)), views);
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
				}
			}
		}
		
		// sort views based on utility and pick top views
		Collections.sort(views, new Comparator<DiscriminatingView>() {
			public int compare(DiscriminatingView a, DiscriminatingView b) {
				return (int) (Math.ceil(b.getUtility() - a.getUtility()));
			}
		});
		List<DiscriminatingView> ret = null;
		if (views.size() > 0) {
			ret = views.subList(0, views.size() > runtimeSettings.numViews ? runtimeSettings.numViews : views.size());
		}
		System.out.println("Time taken by Process:" + (System.nanoTime() - startTime));
		return ret;
	}
	
	public void CreateAndAddDiscriminatingView(String aggregateQueryForQuery, String aggregateQueryForDataset,
			int numAggregates, ArrayList<String> dimensionAttributes, ArrayList<String> measureAttributes,
		ArrayList<DiscriminatingView> views) throws SQLException {
		ArrayList<ArrayList<DistributionUnit>> queryDists = GetDistributionForQueryMultipleAggregates(
				aggregateQueryForQuery, numAggregates);
		ArrayList<ArrayList<DistributionUnit>> dataDists = GetDistributionForQueryMultipleAggregates(
				aggregateQueryForDataset, numAggregates);
		for (int k = 0; k < numAggregates; k++) {
			DiscriminatingView discView = new DiscriminatingView(dimensionAttributes.get(k), 
					measureAttributes.get(k), queryDists.get(k), dataDists.get(k));
			discView.computeUtility(this.runtimeSettings.metric);
			views.add(discView);
		}
	}
	
	
	/**
	 * Runs the given aggregate/groupby query, normalizes the result and formats into vector
	 * @param distQuery
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<DistributionUnit> GetDistributionForQuery(String distQuery) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		ArrayList<DistributionUnit> dist = new ArrayList<DistributionUnit>();
		while (rs.next()) {					
			dist.add(new DistributionUnit(rs.getObject(1), rs.getDouble(2)));
		}
		
		// normalize
		double total = 0;
		for (DistributionUnit unit : dist) {
			total += unit.fraction;
		}
		for (DistributionUnit unit : dist) {
			unit.fraction /= total;		
		}
		for (DistributionUnit unit : dist) {
			System.out.println(unit);
		}
		System.out.println();
		System.out.println();
		
		
		return dist;
	}
	
	public ArrayList<ArrayList<DistributionUnit>> GetDistributionForQueryMultipleAggregates(String distQuery, 
			int numAggregates) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		ArrayList<ArrayList<DistributionUnit>> dist_list = new ArrayList<ArrayList<DistributionUnit>>();
		for (int i = 0; i < numAggregates; i++)
		{
			dist_list.add(new ArrayList<DistributionUnit>());
		}
		while (rs.next()) {		
			for (int i = 0; i < numAggregates; i++)
			{
				dist_list.get(i).add(new DistributionUnit(rs.getObject(1), rs.getDouble(i + 2)));
			}
		}
		
		
		for (ArrayList<DistributionUnit> dist : dist_list)
		{
			double total = 0;
			for (DistributionUnit unit : dist) {
				total += unit.fraction;
			}
			for (DistributionUnit unit : dist) {
				unit.fraction /= total;		
			}
		}		
		return dist_list;
	}

}
