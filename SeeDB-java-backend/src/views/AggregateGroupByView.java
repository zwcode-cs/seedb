package views;

import com.google.common.base.Joiner;

import common.DifferenceQuery;
import common.ExperimentalSettings;
import common.ExperimentalSettings.DifferenceOperators;

public class AggregateGroupByView extends AggregateView {

	public AggregateGroupByView(DifferenceQuery dq) {
		super(dq);
	}

	@Override
	public String serializeView() {
		String ret = "";
		ret += "diff_type:aggregate_diff;";
		ret += "aggregateValues:";
		for (String key : this.aggregateIdx.keySet()) {
			ret+= key + ":" + this.aggregateIdx.get(key) + ",";
		}
		ret += ";";
		ret += "groupByValues:" + Joiner.on(",").join(this.groupByAttributes) + ";";
		ret += "dataset_num:1;[[";
		for (String key: this.groupByValues.keySet()) {
			ret += key + ":" + Joiner.on(",").join(groupByValues.get(key).get(0)) + ";";
		}
		ret += "]];";
		ret += "dataset_num:2;[[";
		for (String key: this.groupByValues.keySet()) {
			ret += key + ":" + Joiner.on(",").join(groupByValues.get(key).get(1)) + ";";
		}
		ret+= "]];";
		return ret;
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.AGGREGATE;
	}
}
