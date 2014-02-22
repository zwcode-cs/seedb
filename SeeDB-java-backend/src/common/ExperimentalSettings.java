package common;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * This class contains the settings for each run of SeeDB. It is mainly used for
 * experimentation, in other cases, supplied default instance is used
 * @author manasi
 *
 */
public class ExperimentalSettings {
	public enum ComparisonType {TWO_DATASETS, ONE_DATASET_FULL, 
		ONE_DATASET_DIFF};
	public ComparisonType comparisonType = ComparisonType.ONE_DATASET_FULL;
	public enum DifferenceOperators {ALL, AGGREGATE, CARDINALITY, A_B_TESTING, CLASSIFICATION, DATA_SAMPLE};
	public List<DifferenceOperators> differenceOperators;
	
	public int rowSampleSize = 10;
	public boolean noAggregateQueryOptimization = false;
	public boolean optimizeAll = true;
	public boolean combineMultipleAggregates = true;
	public boolean combineMultipleGroupBys = true;
	public boolean mergeQueries = false;
	public int groupBySize = 1;
	public boolean useBinPacking = false;
	public int maxGroupBySize = 2;
	
	/**
	 * Get default settings for SeeDB
	 * @return settings object
	 */
	public static ExperimentalSettings getDefault() {
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.ALL);
		return settings;
	}
}
