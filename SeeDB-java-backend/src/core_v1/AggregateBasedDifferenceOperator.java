package core_v1;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import utils.Constants;
import utils.DistributionUnit;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import core.DiscriminatingView;
import core.QueryExecutor;


public class AggregateBasedDifferenceOperator extends DifferenceOperator {
	private ArrayList<AggregateOptimization> optimizations;
	private int maxDimOfViewQuery;

	public AggregateBasedDifferenceOperator(InputQuery inputQuery1, 
			int maxDimOfViewQuery) {
		super(inputQuery1);
		this.maxDimOfViewQuery = maxDimOfViewQuery;
		optimizations = new ArrayList<AggregateOptimization>();
	}
	
	public AggregateBasedDifferenceOperator(InputQuery inputQuery1, 
			InputQuery inputQuery2, int maxDimOfViewQuery) {
		super(inputQuery1, inputQuery2);
		this.maxDimOfViewQuery = maxDimOfViewQuery;
		optimizations = new ArrayList<AggregateOptimization>();
	}
	
	public void addOptimization(AggregateOptimization opt) {
		optimizations.add(opt);
	}
	
	public void executeQueryAndProcessResults(String query, 
			Map<String, ViewQueryResult> viewQueryMap, 
			ViewQueryBarebones viewQueryBarebones, boolean twoQueriesAsOne, 
			boolean isInputQuery1) throws SQLException {
		ResultSet rs = QueryExecutor.executeQuery(query);
		// get result dictionary and process
		while (rs.next()) {
			ViewQueryResultRow row = new ViewQueryResultRow();
			int size = (twoQueriesAsOne ? 
					viewQueryBarebones.getGroupByAttributes().size() - 1 : 
						viewQueryBarebones.getGroupByAttributes().size());
			for (int j = 0; j < size; j++) {
				row.addGroupByValue(
						viewQueryBarebones.getGroupByAttributes().get(j), 
						rs.getString(j + 1));
			}
			int numDims = viewQueryBarebones.getGroupByAttributes().size() + 1;
			row.setGroup((twoQueriesAsOne ? rs.getInt(numDims) : 
					(isInputQuery1 ? Constants.group1 : Constants.group2)));		
			for (int j = 0; j < 
					viewQueryBarebones.getAggregateAttributes().size(); j++) {
				row.addAggregateValue(
					viewQueryBarebones.getAggregateAttributes().get(j), 
					rs.getDouble(j + numDims));
			}
			
			// process result in streaming manner
			processResultRow(viewQueryMap, row);
		}
	}
	
	public void processResultRow(Map<String, ViewQueryResult> viewQueryMap,
			ViewQueryResultRow row) {
		for (int i = optimizations.size()-1; i >=0; i--) {
			ViewQueryResultRow res = optimizations.get(i
					).processResultsStreaming(viewQueryMap, row);
			if (res != null) processResultRow(viewQueryMap, res);
		}
	}

	@Override
	public List<DiscriminatingView> computeDifference() throws SQLException {
		List<DiscriminatingView> result = Lists.newArrayList();
		Map<String, ViewQueryResult> viewQueryMap = Maps.newHashMap();
		
		// compute all view queries whose results we would like to track. 
		createBareboneViewQueries(viewQueryMap);
		
		// write DB queries
		ArrayList<ViewQuery> viewQueries = writeDBQueries();
		
		// convert to sql and run
		QueryExecutor.Instantiate();
		for (int i = 0; i < viewQueries.size(); i++) {
			ViewQuery viewQuery = viewQueries.get(i);
			if (viewQueries.get(i).getTwoQueriesAsOne()) {
				String query = viewQueries.get(i).convertToSQLQuery();
				executeQueryAndProcessResults(query, viewQueryMap, 
						viewQuery.getViewQueryBarebones(),
						viewQuery.getTwoQueriesAsOne(), true /*isInputQuery1*/);			
			}
			else {
				String query1 = viewQueries.get(i).convertToSQLQuery();
				String query2 = viewQueries.get(i+1).convertToSQLQuery();
				executeQueryAndProcessResults(query1, viewQueryMap, 
						viewQuery.getViewQueryBarebones(),
						viewQuery.getTwoQueriesAsOne(), true /*isInputQuery1*/);			
				executeQueryAndProcessResults(query2, viewQueryMap, 
						viewQuery.getViewQueryBarebones(),
						viewQuery.getTwoQueriesAsOne(), false /*isInputQuery1*/);
				i++;
			}
		}
		

		for (int j = optimizations.size()-1; j >= 0; j--) {
			optimizations.get(j).postProcessResults(viewQueryMap);
		}
		for (ViewQueryResult r : viewQueryMap.values()) {
			DiscriminatingView view = createDiscriminatingView(r);
			result.add(view);
		}
		
		Collections.sort(result, new Comparator<DiscriminatingView>() {
            public int compare(DiscriminatingView a, DiscriminatingView b) {
                    return (int) (Math.ceil(b.getUtility() - a.getUtility()));
            }
		});
		
		return result; // returns all disc views
	}

	private ArrayList<ViewQuery> writeDBQueries() {
		// rewrite queries to be sent to the database
		ArrayList<ViewQuery> viewQueries = new ArrayList<ViewQuery>();
		for (AggregateOptimization optimization : optimizations) {
			optimization.rewriteQueries(viewQueries);
		}
		return viewQueries;
	}

	private void createBareboneViewQueries(
			Map<String, ViewQueryResult> viewQueryMap) {
		List<ViewQueryBarebones> tmp = Lists.newArrayList();
		// Note: no splitting up aggregates for efficiency
		for (AggregateOptimization optimization : optimizations) {
			optimization.createBareboneViewQueries(tmp);
		}
		
		// each view query result is identified by the set of group by and 
		// aggregate attributes it stores
		for (ViewQueryBarebones viewQueryBarebones : tmp) {
			ViewQueryResult viewQueryResult = new ViewQueryResult(viewQueryBarebones);
			viewQueryMap.put(viewQueryBarebones.toString(), viewQueryResult);
		}
	}

	private DiscriminatingView createDiscriminatingView(
			ViewQueryResult result) {
		List<DistributionUnit> query1Distribution = Lists.newArrayList();
		List<DistributionUnit> query2Distribution = Lists.newArrayList();
		
		for (ViewQueryResultRow row : result.getRows().values()) {
			DistributionUnit unit = new DistributionUnit(
				Joiner.on(Constants.spacer).join(
						row.getGroupByAttributeValues()),
					row.getAggregateValue(result.getViewQueryBarebones(
							).getAggregateAttributes().get(0)));		
			if (row.getGroup() == Constants.group1)
				query1Distribution.add(unit);
			else 
				query2Distribution.add(unit);
		}
		normalizeDistribution(query1Distribution);
		normalizeDistribution(query2Distribution);
		DiscriminatingView view = new DiscriminatingView(
				result.getViewQueryBarebones().getAggregateAttributes(), 
				result.getViewQueryBarebones().getGroupByAttributes(),
				query1Distribution, query2Distribution);
		view.computeUtility("EarthMoverDistance");
		return view;
	}
	
	private void normalizeDistribution(List<DistributionUnit> dist) {
		double total = 0;
        for (DistributionUnit unit : dist)
                total += unit.fraction;
        for (DistributionUnit unit : dist)
                unit.fraction /= total;
	}

	public int getMaxDimOfViewQuery() {
		return this.maxDimOfViewQuery;
	}
}
