package views;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

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

	public Map<String, Integer> getAggregateAttributeIndex() {
		return this.aggregateIdx;
	}

	public List<String> getGroupByAttributes() {
		return this.groupByAttributes;
	}

	public HashMap<String, List<List<Double>>> getResult() {
		return this.groupByValues;
	}
	
	public String toString() {
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
}
