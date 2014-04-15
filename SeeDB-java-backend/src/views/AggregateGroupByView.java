package views;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import common.DifferenceQuery;
import common.ExperimentalSettings.DifferenceOperators;

public class AggregateGroupByView extends AggregateView {

	public AggregateGroupByView(DifferenceQuery dq) {
		super(dq);
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.AGGREGATE;
	}

	public Set<String> getAggregateAttributeIndex() {
		return this.aggregateIdx.keySet();
	}

	public List<String> getGroupByAttributes() {
		return this.groupByAttributes;
	}

	public HashMap<String, List<List<Double>>> getResult() {
		return this.groupByValues;
	}
}
