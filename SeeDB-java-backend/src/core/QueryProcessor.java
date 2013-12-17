package core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
	private Metadata metadata;
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
		metadata = new Metadata(table);
		try {
			metadata.updateTableSchema();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		ParseQuery();
	}

	public String getQuery() {
		return query;
	}

	public Metadata getMetadata() {
		return metadata;
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
	

	public String queryWithViewPredicates(String dimensionAttribute, List<String> measureAttributes, boolean queryOnly) {
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
		
		// get metadata about table mentioned in the query
		Metadata tableMetadata = getMetadata();
		
		List<String> dimensionAttributes = tableMetadata.getDimensionAttributes();
		List<String> measureAttributes = tableMetadata.getMeasureAttributes();
		List<DiscriminatingView> views = Lists.newArrayList();
		
		for (int dimensionIndex = 0; dimensionIndex < dimensionAttributes.size(); dimensionIndex++) {
			if (dimensionAttributes.get(dimensionIndex).equalsIgnoreCase(selectPredicate)) {
				continue;
			}
			if (runtimeSettings.useMultipleAggregateSingleGroupByOptimization) {
				String aggregateQueryForQuery = queryWithViewPredicates(dimensionAttributes.get(dimensionIndex), 
						measureAttributes, true);
				String aggregateQueryForDataset = queryWithViewPredicates(dimensionAttributes.get(dimensionIndex), 
						measureAttributes, false);
				try {
					CreateAndAddDiscriminatingView(aggregateQueryForQuery, aggregateQueryForDataset, 
							Lists.newArrayList(dimensionAttributes.get(dimensionIndex)), measureAttributes, views);
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}
					
			} else {		
				for (int measureIndex = 0; measureIndex < measureAttributes.size(); measureIndex++) {	
					// add an aggregate and group by: currently on SUM
					String aggregateQueryForQuery = queryWithViewPredicates(dimensionAttributes.get(dimensionIndex), 
							Lists.newArrayList(measureAttributes.get(measureIndex)), true);
					String aggregateQueryForDataset = queryWithViewPredicates(dimensionAttributes.get(dimensionIndex), 
							Lists.newArrayList(measureAttributes.get(measureIndex)), false);
					try {
						CreateAndAddDiscriminatingView(aggregateQueryForQuery, aggregateQueryForDataset,
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
			List<String> dimensionAttributes, List<String> measureAttributes,
		List<DiscriminatingView> views) throws SQLException {
		
		int numAggregates = measureAttributes.size();
		List<List<DistributionUnit>> queryDists = GetDistributionForQueryMultipleAggregates(
				aggregateQueryForQuery, numAggregates);
		List<List<DistributionUnit>> dataDists = GetDistributionForQueryMultipleAggregates(
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
	public List<DistributionUnit> GetDistributionForQuery(String distQuery) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		List<DistributionUnit> dist = Lists.newArrayList();
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
	
	public List<List<DistributionUnit>> GetDistributionForQueryMultipleAggregates(String distQuery, 
			int numAggregates) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(distQuery);
		List<List<DistributionUnit>> distList = Lists.newArrayList();
		
		for (int i = 0; i < numAggregates; i++) {
			distList.add(new ArrayList<DistributionUnit>());
		}
		
		while (rs.next()) {		
			for (int indexOfAggregate = 0; indexOfAggregate < numAggregates; indexOfAggregate++)
			{
				int indexOfAggregateInRow = indexOfAggregate + 2;
				distList.get(indexOfAggregate).add(new DistributionUnit(rs.getObject(1), rs.getDouble(indexOfAggregateInRow)));
			}
		}
		
		// makes sure distribution adds up to 1
		for (List<DistributionUnit> dist : distList)
		{
			double total = 0;
			for (DistributionUnit unit : dist) {
				total += unit.fraction;
			}
			for (DistributionUnit unit : dist) {
				unit.fraction /= total;		
			}
		}		
		return distList;
	}
	
	public Map<String, List<DistributionUnit>> getDistributionsForAllDimensions(int maxValues) throws SQLException {
		Map<String, List<DistributionUnit>> distributions = Maps.newHashMap();
		
		for(String dimensionAttribute : metadata.getDimensionAttributes()) {
			// query to get <maxValues> most common values for column <measureAttribute>
			String query = "select count(*), " + dimensionAttribute + " from " + table + " group by " + dimensionAttribute + " order by count(*) desc limit " + maxValues + ";";
			ResultSet rs = QueryExecutor.executeQuery(query); // rs should have rows with count, value
			
			List<DistributionUnit> distributionForCurrentDimension = Lists.newArrayList();
			while (rs.next()) {
				int count = rs.getInt(1);
				Object value = rs.getObject(2);
				
				distributionForCurrentDimension.add(new DistributionUnit(value, count));
			}
			
			distributions.put(dimensionAttribute, distributionForCurrentDimension);
		}
		
		return distributions;
	}
	
	/**
	 * Gets all data required to draw all charts on frontend
	 * @return A list with one map per combination of dimensions; keys are column names and values are aggregated values of that column
	 * @throws SQLException
	 */
	public List<Map<String, Object>> getAllMeasureAggregatesForAllDimensionCombinations() throws SQLException {
		List<Map<String, Object>> resultRows = Lists.newArrayList();
		
		// select cand_nm, contbr_st, SUM(contb_receipt_amt) from election_data_full group by cand_nm, contbr_st;
		String commaSeparatedDimensionColumns = Joiner.on(", ").join(metadata.getDimensionAttributes()); // cand_nm, contbr_st
		
		List<String> measuresWrappedWithSum = Lists.transform(metadata.getMeasureAttributes(), new Function<String, String>() {
			public String apply(String measureAttribute) {
				return "SUM(" + measureAttribute + ")";
			}
		});
		
		String commaSeparatedSumMeasureColumns = Joiner.on(", ").join(measuresWrappedWithSum);
		String query = "select " + commaSeparatedDimensionColumns + ", " + commaSeparatedSumMeasureColumns +
			" from " + table + " where random() < " + runtimeSettings.samplePercent + " group by " + commaSeparatedDimensionColumns + " order by " + commaSeparatedSumMeasureColumns +" desc limit 4000;";
		
		// listOfKeys is a list of column names in the order that the query will return them
		List<String> listOfKeys = Lists.newArrayList();
		listOfKeys.addAll(metadata.getDimensionAttributes());
		listOfKeys.addAll(metadata.getMeasureAttributes());
		
		ResultSet resultSet = QueryExecutor.executeQuery(query);
		
		while(resultSet.next()) {
			Map<String, Object> row = Maps.newHashMap();
			
			for(int i=0; i<listOfKeys.size(); i++) {
				String key = listOfKeys.get(i);
				Object value = resultSet.getObject(i + 1);
				
				row.put(key, value);
			}
			
			resultRows.add(row);
		}
		
		return resultRows;
	}

}
