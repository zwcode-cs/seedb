package difference_operators;

import java.util.List;

import settings.ExperimentalSettings;

import com.google.common.collect.Lists;

import common.Attribute;
import common.DifferenceQuery;
import common.InputQuery;
import common.InputTablesMetadata;

public class RowSampleDifferenceOperator implements DifferenceOperator {

	@Override
	public List<DifferenceQuery> getDifferenceQueries(
			InputQuery[] inputQueries, InputTablesMetadata[] queryMetadatas, 
			int numDatasets, ExperimentalSettings settings) {
		List<DifferenceQuery> queries = Lists.newArrayList();
		DifferenceQuery dq = new DifferenceQuery(
				ExperimentalSettings.DifferenceOperators.DATA_SAMPLE, inputQueries);
		dq.selectAttributes.add(Attribute.selectAllAttribute());
		dq.limitClause = "LIMIT " + settings.rowSampleSize;
		queries.add(dq);
		return queries;
	}

}
