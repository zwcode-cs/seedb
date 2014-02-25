package core_v1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.google.common.base.Joiner;
import utils.CommonOperations;
import utils.Constants;

public class GroupByOptimization extends AggregateOptimization {
	private int numOfGroupByForViewQuery; // number of group bys that must be present in each view query
	private int maxNumOfGroupByForDBMS; // maximum number of group bys attributes that the database can handle without suffering perf problems
	private int maxCardinalityOfGroupBy; // (more accurate) maximum number of individual groups that the database can handle
	private int type;
	
	public static int ENUMERATE_ALL_GROUPBYS = 1;
	public static int LIMIT_NUM_GROUPBY_ATTRIBUTES = 2;
	public static int LIMIT_GROUPBY_CARDINALITY = 3;
	
	public GroupByOptimization(int maxNumOfGroupByForDBMS, 
			int maxCardinalityOfGroupBy,
			int type,
			AggregateBasedDifferenceOperator operator) {
		super(operator);
		this.maxNumOfGroupByForDBMS = maxNumOfGroupByForDBMS;
		this.numOfGroupByForViewQuery = operator.getMaxDimOfViewQuery();
		this.maxCardinalityOfGroupBy = maxCardinalityOfGroupBy;
		this.type = type;
	}
	
	public List<List<String>> optimizeGroupBys(int type) {
		ArrayList<String> dimensionAttributes = 
				this.operator.allMetadata.getDimensionAttributes();
		if (type == ENUMERATE_ALL_GROUPBYS) {
			return CommonOperations.getCombinations(
					this.numOfGroupByForViewQuery, 
					dimensionAttributes);
		}
		// TODO: implement these
		/**
		else if (type == LIMIT_ON_NUM_GROUPBY_ATTRIBUTES) {
			List<List<String>> result = new ArrayList<List<String>>();
			if (maxNumOfGroupByForDBMS < maxNumOfGroupByForViewQuery) {
				return result;
			}
			result.addAll(CommonOperations.divideList(dimensionAttributes, 
					maxNumOfGroupByForDBMS));
			
			return result;
		}
		else if (type == LIMITED_ON_GROUPBY_CARDINALITY) {
			List<List<String>> result = new ArrayList<List<String>>();
			return result;
		}
		*/
		else {
			return null;
		}
	}
	
	@Override
	public void rewriteQueries(List<ViewQuery> queries) {
		// compute the right way to divide the attributes into group bys
		if (!queries.isEmpty()) return; // TODO:GROUP BY must be first. FIX
		List<List<String>> groupByAttributesList = 
				optimizeGroupBys(type);
		for (List<String> groupByAttributes : groupByAttributesList) {
			ViewQueryBarebones query = new ViewQueryBarebones();
			query.getGroupByAttributes().addAll(groupByAttributes);
			queries.add(new ViewQuery(query)); // create full view query 
											   // from barebones and add
		}
	}

	@Override
	public ViewQueryResultRow processResultsStreaming(Map<String, ViewQueryResult> results, 
			ViewQueryResultRow row) {
		List<List<String>> combinations = 
			CommonOperations.getCombinationsMaxSize(
				this.numOfGroupByForViewQuery, 
				CommonOperations.setToList(
					row.getGroupByAttributes()));
		for (List<String> combination : combinations) {
			String key = Joiner.on(Constants.spacer).join(combination) + 
					Constants.spacer + Constants.spacer +  
					Joiner.on(Constants.spacer).join(
							row.getAggregateAttributes());
			ViewQueryResult viewQueryResult = results.get(key);
			// if viewQueryResult rows contains this combination of GBys, 
			// update; else insert
			String serializedGroupByValues = row.getSerializedGroupByValues(combination);
			if (viewQueryResult.getRows().containsKey(serializedGroupByValues))
			{
				ViewQueryResultRow mapRow = 
					viewQueryResult.getRows().get(serializedGroupByValues); 
				for (String k : row.getAggregateAttributes()) {
					mapRow.updateAggregateValue(k, 
						mapRow.getAggregateValue(k) + 
						row.getAggregateValue(k));
				}
			} else {
				ViewQueryResultRow newRow = new ViewQueryResultRow(row);
				ArrayList<String> toRemove = new ArrayList<String>();
				for (String k : newRow.getGroupByAttributes()) {
					if (!combination.contains(k))
						toRemove.add(k);
				}
				for (String k : toRemove) {
					newRow.removeGroupByValue(k);
				}
				viewQueryResult.addResultRow(newRow);
			}
		}
		return null;
	}

	@Override
	public void postProcessResults(Map<String, ViewQueryResult> results) {
		return;
	}

	@Override
	public void createBareboneViewQueries(List<ViewQueryBarebones> queries) {
		List<List<String>> combinations = 
				CommonOperations.getCombinationsMaxSize(
						numOfGroupByForViewQuery, 
						this.operator.allMetadata.getDimensionAttributes());
		for (List<String> combination : combinations) {
			ViewQueryBarebones viewQueryBarebones = new ViewQueryBarebones();
			viewQueryBarebones.getGroupByAttributes().addAll(combination);
			queries.add(viewQueryBarebones);
		}
	}
}
