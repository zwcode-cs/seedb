package core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
	
	public static final int QUERY = 1;
	public static final int FULL = 2;
	public static final int BOTH = 3;
	
	public RuntimeSettings getRuntimeSettings() {
		return runtimeSettings;
	}
	
	public void setRuntimeSettings(RuntimeSettings runtimeSettings) {
		this.runtimeSettings = runtimeSettings;
	}
	
	public String getTable() {
		return table;
	}
	
	public String getSelectPredicate() {
		return selectPredicate;
	}
	
	public QueryProcessor() {
		QueryExecutor.Instantiate();
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getQuery() {
		return query;
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
	
	/**
	 * Modifies the query string to add aggregates, groupby and sampling operations
	 * @param dimensionAttributes
	 * @param measureAttributes
	 * @param queryOnly
	 * @return
	 */
	public String AddViewPredicates(Object dimensionAttributes[], Object[] measureAttributes, int type) {
		// update the query for the aggregate and group by
		int where_idx = query.indexOf("where");
		int end_idx = query.indexOf(";");
		String wherePredicate = query.substring(where_idx + 5, end_idx).trim();
		
		//// SELECT part ////
		String viewQuery = "select ";
		for (Object dimensionAttribute : dimensionAttributes) {
			viewQuery += dimensionAttribute.toString() + ", ";
		}
		if (type == BOTH) {
			viewQuery += "case when " + wherePredicate + " then 1 else 0 end";
		} else {
			viewQuery = viewQuery.substring(0, viewQuery.length() - 2); // remove extraneous ', '
		}
		
		for (Object measureAttribute : measureAttributes) {
			viewQuery += ", SUM(" + measureAttribute.toString() + ")";
		}
		viewQuery += " ";
		//// SELECT part ////
		
		//// FROM part ////
		viewQuery += "from " + table + " ";
		//// FROM part ////
		
		//// WHERE part ////
		viewQuery += (type == QUERY ? "where " + wherePredicate + " ": "");
		if (runtimeSettings.useSampling) {
			if (type == QUERY) viewQuery += "AND random() < ";
			else viewQuery += "WHERE random() < ";
			viewQuery += runtimeSettings.samplePercent + " ";
		}
		//// WHERE part ////
		
		//// GROUPBY part ////
		viewQuery += "GROUP BY ";
		for (Object dimensionAttribute : dimensionAttributes) {
			viewQuery += dimensionAttribute.toString() + ", ";
		}
		
		if (type == BOTH) {
			viewQuery += "case when " + wherePredicate + " then 1 else 0 end";
		} else {
			viewQuery = viewQuery.substring(0, viewQuery.length() - 2); // remove extraneous ', '
		}
		//// GROUPBY part ////
		
		viewQuery += ";";
		return viewQuery;
	}
	
	public List<DiscriminatingView> Process() {
		long startTime = System.nanoTime();
		
		// parse query for table and selectPredicate
		ParseQuery();
		// get metadata about table mentioned in the query
		Metadata tableMetadata = new Metadata(table);
		try {
			tableMetadata.getTableSchema();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		ArrayList<String> dimensionAttributes = tableMetadata.getDimensionAttributes();
		ArrayList<String> measureAttributes = tableMetadata.getMeasureAttributes();
		ArrayList<DiscriminatingView> views = new ArrayList<DiscriminatingView>();
		
		for (int i = 0; i < dimensionAttributes.size(); i++) {
			if (dimensionAttributes.get(i).equalsIgnoreCase(selectPredicate)) {
				continue;
			}
			if (runtimeSettings.useMultipleAggregateSingleGroupByOptimization) {
				if (runtimeSettings.useCombineSubsetAndFullQueries) {
					views.addAll(ProcessCombinedAggregateQuery(new Object[] {dimensionAttributes.get(i)}, measureAttributes.toArray()));
				} else {
					try {
						views.addAll(ProcessSeparateAggregateQueries(new Object[] {dimensionAttributes.get(i)}, 
								measureAttributes.toArray()));
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
				}
					
			} else {		
				for (int j = 0; j < measureAttributes.size(); j++) {	
					// add an aggregate and group by: currently on SUM
					if (runtimeSettings.useCombineSubsetAndFullQueries) {
						views.addAll(ProcessCombinedAggregateQuery(new Object[] {dimensionAttributes.get(i)}, 
								new Object[] {measureAttributes.get(j)}));
					} else {
						try {
							views.addAll(ProcessSeparateAggregateQueries(new Object[] {dimensionAttributes.get(i)}, 
									new Object[] {measureAttributes.get(j)}));
						} catch (Exception e) {
							e.printStackTrace();
							return null;
						}
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
	
	private ArrayList<DiscriminatingView> ProcessSeparateAggregateQueries(
			Object[] dimensionAttributes, Object[] measureAttributes) throws SQLException {
		ArrayList<DiscriminatingView> views = new ArrayList<DiscriminatingView>();
		String aggregateQueryForQuery = AddViewPredicates(dimensionAttributes, measureAttributes, QUERY);
		String aggregateQueryForDataset = AddViewPredicates(dimensionAttributes, measureAttributes, FULL);
		CreateAndAddDiscriminatingView(aggregateQueryForQuery, aggregateQueryForDataset, measureAttributes.length, 
				dimensionAttributes, measureAttributes, views);
		return views;
	}

	private ArrayList<DiscriminatingView> ProcessCombinedAggregateQuery(Object[] dimensionAttributes,
			Object[] measureAttributes) {
		ArrayList<DiscriminatingView> views = new ArrayList<DiscriminatingView>();
		// write the query
		String aggregateQueryForQuery = AddViewPredicates(dimensionAttributes, measureAttributes, BOTH);
		// run the query
		
		// process result
		
		// create and add discriminating view
		
		return views;
	}

	public void CreateAndAddDiscriminatingView(String aggregateQueryForQuery, String aggregateQueryForDataset,
			int numAggregates, Object[] dimensionAttributes, Object[] measureAttributes,
		ArrayList<DiscriminatingView> views) throws SQLException {
		ArrayList<ArrayList<DistributionUnit>> queryDists = GetDistributionForQueryMultipleAggregates(
				aggregateQueryForQuery, numAggregates);
		ArrayList<ArrayList<DistributionUnit>> dataDists = GetDistributionForQueryMultipleAggregates(
				aggregateQueryForDataset, numAggregates);
		for (int k = 0; k < numAggregates; k++) {
			DiscriminatingView discView = new DiscriminatingView(measureAttributes[k].toString(), 
					dimensionAttributes[0].toString(), queryDists.get(k), dataDists.get(k));
			discView.computeUtility(runtimeSettings.metric);
			views.add(discView);
		}
	}
	
	
	/**
	 * Runs the given aggregate/groupby query, normalizes the result and formats into vector
	 * @param distQuery
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<ArrayList<DistributionUnit>> GetDistributionForQueryMultipleAggregates(String distQuery, 
			int numAggreagtes) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		ArrayList<ArrayList<DistributionUnit>> dist_list = new ArrayList<ArrayList<DistributionUnit>>();
		for (int i = 0; i < numAggreagtes; i++)
		{
			dist_list.add(new ArrayList<DistributionUnit>());
		}
		while (rs.next()) {		
			for (int i = 0; i < numAggreagtes; i++)
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
	
	public ArrayList<ArrayList<DistributionUnit>> GetDistributionForCombinedQueryMultipleAggregates(String distQuery, 
			int numAggreagtes) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		ArrayList<ArrayList<DistributionUnit>> dist_list = new ArrayList<ArrayList<DistributionUnit>>();
		for (int i = 0; i < numAggreagtes; i++)
		{
			dist_list.add(new ArrayList<DistributionUnit>());
		}
		while (rs.next()) {		
			for (int i = 0; i < numAggreagtes; i++)
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
