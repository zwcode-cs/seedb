package core_v1;

import java.util.List;
import java.util.Map;

import utils.Constants;

import com.google.common.collect.Maps;


public class MergeQueriesOptimization extends AggregateOptimization {
	private boolean writeAsOneQuery;
	private boolean fullDataQuery;
	
	public MergeQueriesOptimization(AggregateBasedDifferenceOperator operator,
			boolean writeAsOneQuery) {
		super(operator);
		this.writeAsOneQuery = writeAsOneQuery;
		if (operator.inputQuery2 == null) {
			this.fullDataQuery = true;
			operator.inputQuery2 = new InputQuery(operator.inputQuery1);
			operator.inputQuery2.setWhereClause(null); // this will not work for joins
													 // TODO: fix for joins
		}
	}
	
	/**
	 * This optimization actually sets the input query that is later used
	 * to generate SQL queries. inputquery is null until this point
	 */
	@Override
	public void rewriteQueries(List<ViewQuery> queries) {
		for (int i = 0; i < queries.size(); i++) {
			ViewQuery viewQuery = queries.get(i);
			if (writeAsOneQuery) {
				viewQuery.setTwoQueriesAsOne(true);
				viewQuery.setInput(new InputQuery(operator.inputQuery1));
				if (fullDataQuery) {
					viewQuery.getInput().setWhereClause(null); //remove where
					viewQuery.getViewQueryBarebones().getGroupByAttributes(
						).add("case when " + 
						operator.inputQuery1.getWhereClause() + 
						"then " + Constants.group1 + " else " + 
						Constants.group2);
				} else {
					viewQuery.getInput().setWhereClause(
						viewQuery.getInput().getWhereClause() + " or " + 
						operator.inputQuery2.getWhereClause());
					viewQuery.getViewQueryBarebones().getGroupByAttributes(
						).add("case when " + 
						operator.inputQuery1.getWhereClause() +
						"then " + Constants.group1 +  "when " + 
						operator.inputQuery2.getWhereClause() + "then " + 
						Constants.group2 + " else " + Constants.group0);
				}
			}
			else {
				// write two separate queries: replicate view query, 
				// add input queries
				ViewQuery dup = viewQuery.clone();
				viewQuery.setInput(operator.inputQuery1);
				dup.setInput(operator.inputQuery2);
				queries.add(i+1, dup);
				i++;
			}
		}
	}

	/**
	 * because of the way see db works for a query vs. entire dataset difference,
	 * when a fulldataquery is written as a single query, the results of the query
	 * must be counted towards the results of the entire dataset
	 * 
	 * so this function creates a copy of the row and returns it for further processing
	 */
	@Override
	public ViewQueryResultRow processResultsStreaming(
			Map<String, ViewQueryResult> viewQueryResults,
			ViewQueryResultRow row) {
		if (fullDataQuery && writeAsOneQuery) {
			if (row.getGroup() == Constants.group1) {
				ViewQueryResultRow newRow = new ViewQueryResultRow(row);
				newRow.setGroup(Constants.group2);
				return newRow;
			}
		}
		return null;
	}

	@Override
	public void postProcessResults(Map<String, ViewQueryResult> results) {
		return; // no post processing required
	}

	@Override
	public void createBareboneViewQueries(List<ViewQueryBarebones> tmp) {
		return;
		
	}
}
