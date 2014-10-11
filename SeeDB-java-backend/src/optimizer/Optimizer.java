package optimizer;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.DifferenceOperators;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.DifferenceQuery;
import common.InputTablesMetadata;
import common.Utils;

public class Optimizer {
	ExperimentalSettings settings;
	File logFile;
	static double ROW_TIME = 1;
	static double COL_TIME = 1;
	static double WRITE_TIME = 5;

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
			if (query.op == DifferenceOperators.DATA_SAMPLE || query.op == DifferenceOperators.CARDINALITY) {
				// no optimization here for now
				optimizedQueries.add(query);
			}
			if (query.op == DifferenceOperators.AGGREGATE) {
				if (settings.mergeQueries) query.mergedQueries = true;
				aggregateQueries.add(query);
			}
		}
		optimizedQueries.addAll(optimizeAggregateQueries(aggregateQueries, queryMetadata));
		Utils.writeToFile(logFile, "Optimizer: " + (System.currentTimeMillis() - start));
		return optimizedQueries;
	}
	
	public List<DifferenceQuery> optimizeAggregateQueries(List<DifferenceQuery> queries, InputTablesMetadata queryMetadata) {
		if (settings.noAggregateQueryOptimization || 
				(!settings.combineMultipleAggregates && 
				 !settings.combineMultipleGroupBys)) 
			return queries;
		
		//System.out.println("Number of queries, no opt:" + queries.size());
		
		List<DifferenceQuery> optimizedQueries = Lists.newArrayList();
		// first combine queries with same group by attributes
		if (settings.optimizeAll || settings.combineMultipleAggregates) {
			HashMap<String, List<DifferenceQuery>> combinerMap = Maps.newHashMap();
			for (DifferenceQuery query : queries) {
				if (combinerMap.containsKey(query.getSerializedGroupByAttributes())) {
					List<DifferenceQuery> l = combinerMap.get(query.getSerializedGroupByAttributes());
					l.add(query);
				}
				else {
					List<DifferenceQuery> l = Lists.newArrayList();
					l.add(query);
					combinerMap.put(query.getSerializedGroupByAttributes(), l);
				}
			}
			combineAggregates(combinerMap.values(), settings.maxAggSize);
			for (List<DifferenceQuery> l : combinerMap.values()) {
				optimizedQueries.addAll(l);
			}
		} else {
			optimizedQueries.addAll(queries);
		}
		//System.out.println("Number of queries after combining queries with same group-by:" + optimizedQueries.size());
		
		// then combine queries w.r.t. group by using bin-packing
		if (settings.optimizeAll || settings.combineMultipleGroupBys) {
			if (settings.useBinPacking) {
				// TODO: implement binpacking
			} else if (settings.useHeuristic) {
				useHuffmanHeuristic(optimizedQueries, queryMetadata);
			}
			else {
				useBasicGroupByCombination(optimizedQueries);		
			}
			//System.out.println("Number of queries after combining group-by:" + optimizedQueries.size());
		}		
		return optimizedQueries;
	}
	
	private void useBasicGroupByCombination(List<DifferenceQuery> optimizedQueries) {
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
	
	private void useHuffmanHeuristic(List<DifferenceQuery> optimizedQueries, InputTablesMetadata queryMetadata) {	
		double oldEstimate = Double.MAX_VALUE;
		double newEstimate;
		// huffman encoding style operation
		// (1 1) 1 3 5 8 10
		// (1 2) 3 5 8 10
		// (3 3) 5 8 10
		// (5 6) 8 10
		
		// 2 5 10 20 30 40
		// 
		
		// when you return, the multiplication of the first two will be the least multiplication. 
		// That is the maximum multiplication possible given your input values
		
		while (true) {
			
			// 1. sort
			Collections.sort(optimizedQueries);
			// 2. combine first two
			List<List<Integer>> groups = Lists.newArrayList();
			if (optimizedQueries.size() >= 2) {
				List<Integer> l = Lists.newArrayList();
				l.add(optimizedQueries.get(0).getMaxDistinct());
				l.add(optimizedQueries.get(1).getMaxDistinct());
				groups.add(l);
			} else {
				break;
			}
			for (int i = 2; i < optimizedQueries.size(); i++) {
				List<Integer> l = Lists.newArrayList();
				l.add(optimizedQueries.get(i).getMaxDistinct());
				groups.add(l);
			}
			// 3. estimate: better, go to 1
			//              worse, return
			newEstimate = estimate(groups, settings.maxDBConnections, 
					queryMetadata.getMeasureAttributes().size(), 
					queryMetadata.getDimensionAttributes().size(), 
					queryMetadata.getNumRows(),
					true /* use average */);
			if (newEstimate < oldEstimate) {
				// combine group bys
				Utils.combine(optimizedQueries.get(0).groupByAttributes, 
						  optimizedQueries.get(1).groupByAttributes);
				// update derived from
				if (optimizedQueries.get(1).derivedFrom.isEmpty()) {
					optimizedQueries.get(0).derivedFrom.add(optimizedQueries.get(1));
				} else {
					optimizedQueries.get(0).derivedFrom.addAll(optimizedQueries.get(1).derivedFrom);
				}
				// remove now redundant query
				optimizedQueries.remove(1);
				oldEstimate = newEstimate;
			} else {
				// cannot optimize further
				break;
			}	
		}
	}
	
	/**
	 * Estimate the time taken to execute the query
	 * @param groups	List of sizes of group-bys for attributes combined together
	 * @param conn		Number of threads executed in parallel
	 * @param nAgg		Total number of measure attributes present in the table
	 * @param nDims		Total number of dimension attributes present in the table
	 * @param nRows		Total number of rows present in the table
	 * @return
	 */
 	private double estimate(List<List<Integer>> groups, int conn, int nAgg, int nDims, int nRows, boolean useAverage) {
		double bcreate = Math.ceil(groups.size() * 1.0 /conn); // number of iterations of create
		double avg_dtt = 0;								// average number of dimensions in temp tables
		double avg_ntt = 0;								// average number of rows in temp tables
		
		if (useAverage) {
			for (List<Integer> group : groups) {
				avg_dtt += group.size();
				avg_ntt += getProductOfList(group);
			}
			avg_dtt /= groups.size();
			avg_ntt /= groups.size();
			double bquery = Math.ceil(avg_dtt * nAgg * Math.min(conn, groups.size()) * 1.0/conn); 	// number of queries per table
			double tquery = avg_ntt * (ROW_TIME + COL_TIME * (avg_dtt + nAgg)); 		// time to create one temp table
			double tcreate = nRows * (ROW_TIME + COL_TIME * (nDims + nAgg)) + WRITE_TIME * avg_ntt * 
						(ROW_TIME + COL_TIME * (avg_dtt + nAgg)); 								// time to query a temp table
			return tcreate + (bcreate - 1) * Math.abs(tcreate - tquery * bquery) + tquery * bquery;
		} else {
			// use maximum across batches and sum up
			List<List<List<Integer>>> batches = createBatches(groups, conn);
			
			// get maximum number of columns for each batch and max number of rows.
			List<Integer> d_tt = Lists.newArrayList();
			List<Integer> n_tt = Lists.newArrayList();
			for (List<List<Integer>> batch : batches) {
				int d_tt_max = -1;
				int n_tt_max = -1;
				for (List<Integer> group : batch) {
					int curr_d_tt_max = group.size();
					if (d_tt_max < curr_d_tt_max) {
						d_tt_max = curr_d_tt_max;
					}
					int curr_n_tt_max = getProductOfList(group);
					if (n_tt_max < curr_n_tt_max) {
						n_tt_max = curr_n_tt_max;
					}
				}
				d_tt.add(d_tt_max);
				n_tt.add(n_tt_max);
			}
			
			// estimate time for each batch and compute total time
			// total = time to create first set of tables
			//		   + <difference of time to query current table and time to create next set of tables> * (number of batches - 1)
			//		   + time to query last set of tables
			double estimate = n_tt.get(0) * (ROW_TIME + COL_TIME*(d_tt.get(0) + nAgg));	// max create time for first batch
			for (int i = 1; i < batches.size()-1; i++) {
				 double queryTime = n_tt.get(i-1) * (ROW_TIME + COL_TIME * d_tt.get(i-1)) 
						 * d_tt.get(i-1) * nAgg * Math.min(conn, batches.get(i-1).size()); 
				 double createTime = n_tt.get(i) * (ROW_TIME + COL_TIME * (d_tt.get(i) + nAgg));	
				 estimate += Math.abs(queryTime - createTime);
			}
			estimate += n_tt.get(batches.size()-1) * (ROW_TIME + COL_TIME * d_tt.get(batches.size()-1)) 
					 * d_tt.get(batches.size()-1) * nAgg * Math.min(conn, batches.get(batches.size()-1).size()); 
			return estimate;
		}
	}

 	/**
 	 * figure out what groups of queries to run together based on size of group bys
 	 * @param groups
 	 * @param conn
 	 * @return
 	 */
	private List<List<List<Integer>>> createBatches(List<List<Integer>> groups,
			int conn) {
		if (groups.isEmpty()) {
			return Lists.newArrayList();
		}
		// batch into groups of size conn (assume that groups are sorted)
		List<List<List<Integer>>> ret = Lists.newArrayList();
		
		// make batches
		List<List<Integer>> temp = null;
		for (int i = 0; i < groups.size(); i++) {
			if (i % conn == 0) {
				temp = Lists.newArrayList();
				temp.add(groups.get(i));
				ret.add(temp);
			} else {
				temp.add(groups.get(i));
			}
		}
		return ret;
	}
	
	private int getProductOfList(List<Integer> l) {
		int ret = 1;
		for (Integer i : l) {
			ret *= i;
		}
		return ret;
	}

	private int getSumOfList(List<Integer> l) {
		int ret = 0;
		for (Integer i : l) {
			ret += i;
		}
		return ret;
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
