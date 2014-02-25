package core_v1;

import java.util.List;
import java.util.Map;

import core.DiscriminatingView;
import core.Metadata;

public abstract class AggregateOptimization {
	protected AggregateOptimization childOptimization;
	AggregateBasedDifferenceOperator operator;
	
	public AggregateOptimization(AggregateBasedDifferenceOperator operator) {
		this.operator = operator;
	}
	
	// create view queries
	public abstract void createBareboneViewQueries(List<ViewQueryBarebones> tmp);
	
	// rewrite queries
	public abstract void rewriteQueries(List<ViewQuery> queries);
	
	// process one query result at a time
	public abstract ViewQueryResultRow processResultsStreaming(
		Map<String, ViewQueryResult> results, ViewQueryResultRow row);
	
	// process query results in bulk (after the results have been processed
	// in streaming fashion)
	public abstract void postProcessResults(
		Map<String, ViewQueryResult> results);
	
	public void setChildOptimization(AggregateOptimization childOptimization) {
		this.childOptimization = childOptimization;
	}
}
