package views;

import common.DifferenceQuery;
import common.ExperimentalSettings;
import common.Utils;
import common.ExperimentalSettings.DifferenceOperators;

public class CardinalityView extends AggregateView {
	public CardinalityView(DifferenceQuery dq) {
		super(dq);
	}

	@Override
	public String serializeView() {
		String ret = "";
		ret += "diff_type:cardinality_diff;";
		ret += "dataset_num:1;";
		ret += "cardinality:" + this.groupByValues.get("none").get(0).get(0) + ";";
		ret += "dataset_num:2;";
		ret += "cardinality:" + this.groupByValues.get("none").get(1).get(0) + ";";
		return ret;
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.CARDINALITY;
	}

}
