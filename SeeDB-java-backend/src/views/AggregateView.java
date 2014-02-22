package views;

import java.util.HashMap;
import java.util.List;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.DifferenceQuery;
import common.ExperimentalSettings.DifferenceOperators;

/**
 * Generic view class for differences that are based on aggregates and groupbys
 * @author manasi
 *
 */
public abstract class AggregateView implements View {
	protected HashMap<String, List<List<Double>>> groupByValues; // serialized 
	// groupby attribute values and the corresponding aggregate values
	protected HashMap<String, Integer> aggregateIdx; // aggregate column name
	// to index of that aggregate in the list of aggregate values in 
	// groupbyvalues
	protected int numAggregates;
	protected List<String> groupByAttributes;
	
	public AggregateView(DifferenceQuery dq) {
		groupByAttributes = DifferenceQuery.getAttributeNames(
				dq.groupByAttributes);
		groupByValues = Maps.newHashMap();
		aggregateIdx = Maps.newHashMap();
		List<String> aggAttrs = 
				DifferenceQuery.getColumnNamesForAggregateAttributes(
						dq.aggregateAttributes, dq.aggregateFunctions);
		int ctr = 0;
		// populate aggregate index hash
		for (String aggAttr : aggAttrs) {
			aggAttr = aggAttr.toLowerCase();
			aggregateIdx.put(aggAttr, ctr);
			ctr ++;
		}
		numAggregates = ctr;
	}
	
	@Override
	public abstract String serializeView();

	@Override
	public abstract DifferenceOperators getOperator();

	/**
	 * return the aggregate values for the given group by values and for the 
	 * given dataset
	 * @param string
	 * @param group : 1 or 2 corresponding to dataset
	 * @return
	 */
	private List<Double> getAggregateValues(String string, int group) {
		string = string.toLowerCase();
		if (!groupByValues.containsKey(string)) {
			List<List<Double>> tmp =Lists.newArrayList();
			tmp.add(createListOfSize(numAggregates));
			tmp.add(createListOfSize(numAggregates));
			groupByValues.put(string, tmp);
		}
		return groupByValues.get(string).get(group-1);
	}
	
	private List<Double> createListOfSize(int size) {
		List<Double> l = Lists.newArrayList();
		for (int i = 0; i < size; i++) {
			l.add(new Double(0));
		}
		return l;
	}

	/**
	 * Add the given value for the specified aggregate column and group by value for the
	 * specified group
	 * @param groupBy
	 * @param agg
	 * @param object
	 * @param group
	 */
	public void addAggregateValue(String groupBy, String agg, Object object, int group) {
		List<Double> tmp = getAggregateValues(groupBy, group);
		int idx = this.aggregateIdx.get(agg);
		tmp.set(idx, tmp.get(idx) + (Double) object);
	}

}
