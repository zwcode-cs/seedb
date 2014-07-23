package common;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * This class contains the settings for each run of SeeDB. It is mainly used for
 * experimentation, in other cases, supplied default instance is used
 * @author manasi
 *
 */
public class ExperimentalSettings {
	public enum ComparisonType {TWO_DATASETS, ONE_DATASET_FULL, 
		ONE_DATASET_DIFF, MANUAL_VIEW};
	public ComparisonType comparisonType = ComparisonType.ONE_DATASET_FULL;
	public enum DifferenceOperators {AGGREGATE, CARDINALITY, DATA_SAMPLE}; //ALL, A_B_TESTING, CLASSIFICATION, 
	public List<DifferenceOperators> differenceOperators;
	
	public int rowSampleSize = 10;
	public boolean noAggregateQueryOptimization = false; 
	public boolean optimizeAll = true; // apply all optimizations
	public boolean combineMultipleAggregates = true;
	public int maxAggSize = 2;
	public boolean combineMultipleGroupBys = true;
	public boolean mergeQueries = true;
	public int groupBySize = 1; // number of group bys in the final views. 1 since we want 1D
	public boolean useBinPacking = false;
	public int maxGroupBySize = 2;
	public int maxDBConnections = 10;
	public boolean useParallelExecution = false;
	public boolean useTempTables = false;
	public String logFile = null;
	public String shared_buff="32MB";

	public boolean useHeuristic = false;
	
	/**
	 * Get default settings for SeeDB
	 * @return settings object
	 */
	public static ExperimentalSettings getDefault() {
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList(DifferenceOperators.DATA_SAMPLE, DifferenceOperators.AGGREGATE, DifferenceOperators.CARDINALITY);
		return settings;
	}
	
	public String getDescriptor() {
		List<String> l = Lists.newArrayList();
		l.add("istc");
		l.add("final");
		l.add(useParallelExecution ? "parallel_" + this.maxDBConnections : "seq");
		if (noAggregateQueryOptimization) {
			l.add("NoOp");
		}
		if (optimizeAll) {
			l.add("ALL");
			l.add(maxGroupBySize + "GB");
			l.add(maxAggSize + "AGG");
		}
		if (!optimizeAll && !noAggregateQueryOptimization) {
			if (combineMultipleAggregates) {
				l.add("MultipleAgg");
				l.add(maxAggSize + "AGG");
			}
			if (combineMultipleGroupBys) {
				l.add("MultipleGB");
				l.add(maxGroupBySize + "GB");
			}
			if (mergeQueries) {
				l.add("Merged");
			}
		}
		if (useTempTables) {
			l.add("TempTables");
		}
		return Joiner.on("_").join(l);
	}
}
