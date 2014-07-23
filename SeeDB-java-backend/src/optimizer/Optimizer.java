package optimizer;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.DifferenceQuery;
import common.ExperimentalSettings;
import common.ExperimentalSettings.DifferenceOperators;
import common.InputTablesMetadata;
import common.Utils;

public class Optimizer {
	ExperimentalSettings settings;
	File logFile;
	static double ROW_TIME = 0;
	static double COL_TIME = 0;
	static double WRITE_TIME = 0;

	public Optimizer(ExperimentalSettings settings, File logFile) {
		this.settings = settings;
		this.logFile = logFile;
	}

	public List<DifferenceQuery> optimizeQueries(List<DifferenceQuery> input_queries, InputTablesMetadata queryMetadata) {
		long start = System.currentTimeMillis();
		List<DifferenceQuery> queries = Lists.newArrayList();
		for (DifferenceQuery dq : input_queries) {
			queries.add(DifferenceQuery.deepCopy(dq));
		}
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
		optimizedQueries.addAll(optimizeAggregateQueries(aggregateQueries, queryMetadata));
		Utils.writeToFile(logFile, "Optimizer: " + (System.currentTimeMillis() - start));
		return optimizedQueries;
	}
	
	public List<DifferenceQuery> optimizeAggregateQueries(List<DifferenceQuery> queries, InputTablesMetadata queryMetadata) {
		if (settings.noAggregateQueryOptimization || 
				(!settings.combineMultipleAggregates && !settings.combineMultipleGroupBys)) 
			return queries;
		
		List<DifferenceQuery> optimizedQueries = Lists.newArrayList();
		
		// first combine queries with same group by attributes
		if (settings.optimizeAll || settings.combineMultipleAggregates) {
			HashMap<String, List<DifferenceQuery>> combinerMap = Maps.newHashMap();
			for (DifferenceQuery query : queries) {
				if (combinerMap.containsKey(query.getSerializedGroupByAttributes())) {
					List<DifferenceQuery> l = combinerMap.get(query.getSerializedGroupByAttributes());
					//q.derivedFrom.add(query);
					//Utils.combineAggregates(q, query);
					l.add(query);
				}
				else {
					List<DifferenceQuery> l = Lists.newArrayList();
					l.add(query);
					combinerMap.put(query.getSerializedGroupByAttributes(), l);
					//DifferenceQuery tmp = DifferenceQuery.deepCopy(query);
					//tmp.derivedFrom.add(query);
					//combinerMap.put(query.getSerializedGroupByAttributes(), tmp);
				}
			}
			combineAggregates(combinerMap.values(), settings.maxAggSize);
			for (List<DifferenceQuery> l : combinerMap.values()) {
				optimizedQueries.addAll(l);
			}
		} else {
			optimizedQueries.addAll(queries);
		}
		
		// then combine queries w.r.t. group by using bin-packing
		if (settings.optimizeAll || settings.combineMultipleGroupBys) {
			if (settings.useBinPacking) {
				// TODO: implement binpacking
			} else if (settings.useHeuristic) {
				double oldEstimate = Double.MAX_VALUE;
				DifferenceQuery lastUpdated = null;
				double newEstimate;
				while (true) {
					Collections.sort(optimizedQueries);
					List<List<Integer>> groups = Lists.newArrayList();
					for (DifferenceQuery dq : optimizedQueries) {
						List<Integer> l = Lists.newArrayList();
						l.add(dq.groupByAttributes.size());
						groups.add(l);
					}
					newEstimate = estimate(groups, settings.maxDBConnections, 
											queryMetadata.getMeasureAttributes().size(), 
											queryMetadata.getDimensionAttributes().size(), 
											queryMetadata.getNumRows());
					if (newEstimate < oldEstimate) {
						// combine first two
						if (optimizedQueries.size() > 1) {
							// combine first two queries
							DifferenceQuery dq = optimizedQueries.get(1);
							lastUpdated = optimizedQueries.get(0);
							lastUpdated.derivedFrom.add(dq);
							lastUpdated.groupByAttributes.addAll(dq.groupByAttributes);
							optimizedQueries.remove(1);
						}
						oldEstimate = newEstimate;
					} else {
						// separate the last updated query into 2
						DifferenceQuery dq = lastUpdated.derivedFrom.get(lastUpdated.derivedFrom.size()-1);
						optimizedQueries.add(dq);
						lastUpdated.groupByAttributes.removeAll(dq.groupByAttributes);
						break;
					}
				}
			}
			else {
				int combinedSize = settings.maxGroupBySize;
				// sort the optimized queries
				HashMap<String, List<DifferenceQuery>> combinerMap = Maps.newHashMap();
				for (DifferenceQuery query : optimizedQueries) {
					String s = Joiner.on("__").join(DifferenceQuery.getAggregateAttributeColumnNames(
							query.aggregateAttributes, query.aggregateFunctions));
					if (combinerMap.containsKey(s)) {
						List<DifferenceQuery> q = combinerMap.get(s);
						q.add(query);
					}
					else {
						List<DifferenceQuery> q = Lists.newArrayList();
						q.add(query);
						combinerMap.put(s, q);
					}
				}
				combineGroups(combinerMap.values(), combinedSize);	
				optimizedQueries.clear();
				for (List<DifferenceQuery> l : combinerMap.values()) {
					optimizedQueries.addAll(l);
				}
			}
		}		
		return optimizedQueries;
	}
	
	private double estimate(List<List<Integer>> groups, int conn, int nAgg, int nDims, int nRows) {
		double bcreate = Math.ceil(groups.size()/conn); // number of iterations of create
		double avg_dtt = 0;
		double avg_ntt = 0;
		for (List<Integer> group : groups) {
			avg_dtt += group.size();
			int product = 1;
			for (Integer i : group) {
				product *= i;
			}
			avg_ntt += product;
		}
		avg_dtt /= groups.size();
		avg_ntt /= groups.size();
		
		double bquery = avg_dtt * nAgg * Math.min(conn, groups.size())/conn; // number of queries per table
		double tcreate = avg_ntt * (ROW_TIME + COL_TIME*(avg_dtt + nAgg)); // time to create one temp table
		double tquery = nRows * (ROW_TIME + COL_TIME*(nDims + nAgg)) + WRITE_TIME * avg_ntt * 
					(ROW_TIME + COL_TIME*avg_dtt); // time to query a temp table
		return tcreate + (bcreate-1)*Math.abs(tcreate - tquery*bquery) + tquery*bquery;
	}

	private void combineAggregates(Collection<List<DifferenceQuery>> values,
			int maxAggSize) {
		for (List<DifferenceQuery> l : values) {
			combineHelper(l, maxAggSize, false);
		}
		
	}

	private void combineGroups(Collection<List<DifferenceQuery>> listOfQueries, int combinedSize) {
		for (List<DifferenceQuery> l : listOfQueries) {
			combineHelper(l, combinedSize, true);
		}
	}
	
	private void combineHelper(List<DifferenceQuery> queries, int combinedSize, boolean combineGB) {
		for (int i = 0; i < queries.size(); i++) {
			DifferenceQuery currDq = queries.get(i);
			int currSize;
			if (combineGB) {
				currSize = currDq.groupByAttributes.size();
			} else {
				currSize = currDq.aggregateAttributes.size();
			}
			if (currSize > combinedSize) continue;
			else {
				for (int j = i+1; j < queries.size(); j++) {
					DifferenceQuery nextDq = queries.get(j);
					int nextSize;
					if (combineGB) {
						nextSize = nextDq.groupByAttributes.size();
					} else {
						nextSize = nextDq.aggregateAttributes.size();
					}
					if (nextSize + currSize > combinedSize) continue;
					Utils.combineAggregates(currDq, nextDq);
					if (combineGB) {
						Utils.combine(currDq.groupByAttributes, nextDq.groupByAttributes);
					}
					if (nextDq.derivedFrom.isEmpty()) {
						currDq.derivedFrom.add(nextDq);
					} else {
						currDq.derivedFrom.addAll(nextDq.derivedFrom);
					}
					
					queries.remove(j);
					j--;
					currSize += nextSize;
					if (currSize == combinedSize) break;
				}
			}
		}
	}

}
