package views;

import java.util.List;

import com.google.common.collect.Lists;

import common.DifferenceQuery;
import common.ExperimentalSettings.DifferenceOperators;

public class CardinalityView extends AggregateView {
	public CardinalityView(DifferenceQuery dq) {
		super(dq);
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.CARDINALITY;
	}

	public List<Double> getCardinalities() {
		if (this.groupByValues == null || this.groupByValues.get("none") == null) {
			return Lists.newArrayList(null, null);
		}
		
		List<List<Double>> cardinalityAggregates = this.groupByValues.get("none");
		
		// do a bunch of stuff to avoid possible null pointer exceptions :[
		List<Double> output = Lists.newArrayList(null, null);
		
		if (cardinalityAggregates.get(0) == null) {
			output.set(0, null);
		} else {
			output.set(0, cardinalityAggregates.get(0).get(0));
		}
		
		if (cardinalityAggregates.get(1) == null) {
			output.set(1, null);
		} else {
			output.set(1, cardinalityAggregates.get(1).get(0));
		}
		
		return output;
	}

}
