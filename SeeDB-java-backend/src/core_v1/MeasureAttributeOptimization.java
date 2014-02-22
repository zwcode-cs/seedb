package core_v1;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import utils.CommonOperations;

public class MeasureAttributeOptimization extends AggregateOptimization {
	int maxNumAggregates;
	
	public MeasureAttributeOptimization(
			AggregateBasedDifferenceOperator operator, 
			int numMeasureAttributeAggregates) {
		super(operator);
		this.maxNumAggregates = numMeasureAttributeAggregates;
	}
	
	public MeasureAttributeOptimization(
			AggregateBasedDifferenceOperator operator) {
		super(operator);
		this.maxNumAggregates = 
				this.operator.allMetadata.getMeasureAttributes().size();
	}
		
	@Override
	public void rewriteQueries(List<ViewQuery> queries) {
		List<String> measureAttributes = 
				operator.allMetadata.getMeasureAttributes();
		List<List<String>> measureAttributeList = 
				CommonOperations.divideList(measureAttributes, 
						maxNumAggregates);				
		for (int i = 0; i < queries.size(); i++) {
			for (int j = 0; j < measureAttributeList.size(); j++) {
				if (j > 0) {
					queries.add(i, queries.get(i-1).clone());
				}
				queries.get(i).getViewQueryBarebones(
						).getAggregateAttributes().clear();
				queries.get(i).getViewQueryBarebones().getAggregateAttributes(
						).addAll(measureAttributeList.get(j));
				if (j > 0) i++; // skip newly added queries
			}
		}
	}

	@Override
	public ViewQueryResultRow processResultsStreaming(Map<String, ViewQueryResult> results,
			ViewQueryResultRow row) {
		return null;
	}

	@Override
	public void postProcessResults(Map<String, ViewQueryResult> results) {
		// walk through results and break them up
		for (Entry<String, ViewQueryResult> entry : results.entrySet()) {
			if (entry.getValue().getViewQueryBarebones(
					).getAggregateAttributes().size() > 1) {
				String key = entry.getKey();
				ViewQueryResult viewQueryResult = results.remove(key);
				for (String aggregateAttribute : 
					viewQueryResult.getViewQueryBarebones(
							).getAggregateAttributes()) {
					ViewQueryResult newResult = 
						new ViewQueryResult(
								viewQueryResult.getViewQueryBarebones());
					newResult.getViewQueryBarebones().getAggregateAttributes(
							).clear();
					newResult.getViewQueryBarebones().getAggregateAttributes(
							).add(aggregateAttribute);
					results.put(newResult.toString(), newResult);
				}
				for (ViewQueryResultRow row : viewQueryResult.getRows().values()) {
					for (String aggregateAttribute : 
						viewQueryResult.getViewQueryBarebones(
								).getAggregateAttributes()) {
						ViewQueryResultRow newRow = 
							ViewQueryResultRow.copyRowWithSpecificAggregate(
									row, aggregateAttribute);
						results.get(newRow.toString()).addResultRow(newRow);
					}
				}
			}
		}
	}

	@Override
	public void createBareboneViewQueries(List<ViewQueryBarebones> queries) {
		List<String> measureAttributes = 
				operator.allMetadata.getMeasureAttributes();
		List<List<String>> measureAttributeList = 
				CommonOperations.divideList(measureAttributes, 
						maxNumAggregates);				
		for (int i = 0; i < queries.size(); i++) {
			for (int j = 0; j < measureAttributeList.size(); j++) {
				if (j > 0) {
					queries.add(i, queries.get(i-1).clone());
				}
				queries.get(i).getAggregateAttributes().clear();
				queries.get(i).getAggregateAttributes().addAll(
						measureAttributeList.get(j));
				if (j > 0) i++; // skip the recently added query
			}
		}
	}

}