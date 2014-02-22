package optimizer;

import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.DifferenceQuery;
import common.ExperimentalSettings;
import common.ExperimentalSettings.DifferenceOperators;
import common.Utils;

public class Optimizer {
	ExperimentalSettings settings;

	public Optimizer(ExperimentalSettings settings) {
		this.settings = settings;
	}

	public List<DifferenceQuery> optimizeQueries(List<DifferenceQuery> queries) {
		List<DifferenceQuery> optimizedQueries = Lists.newArrayList();
		List<DifferenceQuery> aggregateQueries = Lists.newArrayList();
		for (DifferenceQuery query : queries) {
			if (query.op == DifferenceOperators.DATA_SAMPLE) {
				// no optimization here, the queries just select x rows from each dataset
				optimizedQueries.add(query);
			}
			if (query.op == DifferenceOperators.CARDINALITY ||
					query.op == DifferenceOperators.AGGREGATE) {
				if (settings.mergeQueries) query.mergedQueries = true;
				// we can group these together and optimize
				aggregateQueries.add(query);	
			}
		}
		optimizedQueries.addAll(optimizeAggregateQueries(aggregateQueries));
		return optimizedQueries;
	}
	
	public List<DifferenceQuery> optimizeAggregateQueries(List<DifferenceQuery> queries) {
		if (settings.noAggregateQueryOptimization) return queries;
		List<DifferenceQuery> optimizedQueries = Lists.newArrayList();
		HashMap<String, DifferenceQuery> combinerMap = Maps.newHashMap();
		
		// first combine queries with same group by attributes
		if (settings.optimizeAll || settings.combineMultipleAggregates) {
			for (DifferenceQuery query : queries) {
				if (combinerMap.containsKey(query.getSerializedGroupByAttributes())) {
					DifferenceQuery q = combinerMap.get(query.getSerializedGroupByAttributes());
					q.derivedFrom.add(query);
					Utils.combineAggregates(q, query);
				}
				else {
					DifferenceQuery tmp = DifferenceQuery.deepCopy(query);
					tmp.derivedFrom.add(query);
					combinerMap.put(query.getSerializedGroupByAttributes(), tmp);
				}
			}
		}
		optimizedQueries.addAll(combinerMap.values());
		
		// then combine queries w.r.t. group by using bin-packing
		if (settings.optimizeAll || settings.combineMultipleGroupBys) {
			if (settings.useBinPacking) {
				// TODO: implement binpacking
			}
			else {
				int combinedSize = settings.maxGroupBySize;
				combineGroups(optimizedQueries, combinedSize);	
			}
		}
		return optimizedQueries;
	}

	private void combineGroups(List<DifferenceQuery> queries, int combinedSize) {
		for (int i = 0; i < queries.size(); i++) {
			DifferenceQuery currDq = queries.get(i);
			int currSize = currDq.groupByAttributes.size();
			if (currSize > combinedSize) continue;
			else {
				for (int j = i+1; j < queries.size(); j++) {
					DifferenceQuery nextDq = queries.get(j);
					int nextSize = nextDq.groupByAttributes.size();
					if (nextSize + currSize > combinedSize) continue;
					Utils.combineAggregates(currDq, nextDq);
					Utils.combine(currDq.groupByAttributes, nextDq.groupByAttributes);
					currDq.derivedFrom.addAll(nextDq.derivedFrom);
					queries.remove(j);
					j--;
					currSize += nextSize;
					if (currSize == combinedSize) break;
				}
			}
		}
	}

}
