package difference_operators;

import java.util.List;

import settings.ExperimentalSettings;

import com.google.common.collect.Lists;

import common.Attribute;
import common.DifferenceQuery;
import common.InputQuery;
import common.InputTablesMetadata;

public class CardinalityDifferenceOperator implements DifferenceOperator {

	@Override
	public List<DifferenceQuery> getDifferenceQueries(
			InputQuery[] inputQueries, InputTablesMetadata[] queryMetadatas, 
			int numDatasets, ExperimentalSettings settings) {
		List<DifferenceQuery> queries = Lists.newArrayList();
		DifferenceQuery dq = new DifferenceQuery(
				ExperimentalSettings.DifferenceOperators.CARDINALITY, inputQueries);
		List<String> aggFuncs = Lists.newArrayList();
		aggFuncs.add("COUNT");
		dq.addAggregateAttribute(Attribute.selectAllAttribute(), aggFuncs);
		queries.add(dq);
		return queries;
	}

}
