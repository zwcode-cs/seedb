package settings;

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
	public ComparisonType comparisonType = ComparisonType.ONE_DATASET_FULL; // Default SeeDB q vs. entire data
	public enum Backend {POSTGRES, VERTICA, MAIN_MEMORY};
	public enum MainMemoryPruningAlgorithm {
		NONE, // no pruning 
		TOP_K1, // top-k using CIS
		TOP_K2, // works in phases
		TOP_K3, // varys CI, works in phases
		RANDOM, // randomly pick views to update
		MAB1, // hoeffding bounds
		MAB2, // ucb1(rho) bound
		MAB3, // ucb-h algorithm
		MAB4, // sar algorithm
		MAB5  // sar with clearing between phases	
		};
	
	public enum DifferenceOperators {AGGREGATE, CARDINALITY, DATA_SAMPLE}; //ALL, A_B_TESTING, CLASSIFICATION, 
	public List<DifferenceOperators> differenceOperators; 	// ignore for basic tests	
	public int rowSampleSize = 10; 							// ignore for basic tests

	public boolean noAggregateQueryOptimization = false; 	// true = no optimizations
	public boolean optimizeAll = false; 					// false = apply all optimizations
	
	public boolean combineMultipleAggregates = true;		// OPT1: true = multiple measures together
	public int maxAggSize = 2;								// OPT1: combine aggregates UP to maxAggSize
	
	public boolean combineMultipleGroupBys = true;			// OPT2: combine group-bys together in one query
	public int maxGroupBySize = 2;							
	public boolean useBinPacking = false;					// OPT2: ignore for basic tests, not implemented
	public boolean useHeuristic = false;					// OPT2: ignore for basic tests [huffman]
	

	public boolean mergeQueries = true;						// OPT3: combine comparison and target q
	
	public boolean useParallelExecution = true;				// OPT4: parallel execution
	public int maxDBConnections = 40;						// OPT4: actual is double; max POSTGRES = 100
															// OPT4: do not set above 45	

	public int groupBySize = 1; 							// ignore for basic tests 
															// number of group bys in the final views. 1 since we want 1D
	
	public boolean useTempTables = true;					// set this

	public String logFile = null;							// log file for profiling, set if you want
															// else goes to stdout
															// process using processTestOutput.py in scripts
	public enum DistanceMetric {EARTH_MOVER_DISTANCE,
		EUCLIDEAN_DISTANCE, COSINE_DISTANCE, 
		FIDELITY_DISTANCE, CHI_SQUARED_DISTANCE,
		KULLBACK_LEIBLER_DISTANCE}; 
		
	public DistanceMetric distanceMetric 
		= DistanceMetric.EARTH_MOVER_DISTANCE;
	public boolean makeGraphs;								// whether to make graphs
	public Backend backend;									// what backend to use
	public boolean normalizeDistributions = true;			// whether to normalize distribution
	
	// used for in memory implementation
	public int num_rows = -1;								// number of rows to process. default all
	public boolean mainMemoryRandomSample = false;
	public boolean mainMemoryReadFromFile = true;			// whether to read in from file or from memory
	public MainMemoryPruningAlgorithm mainMemoryPruning = MainMemoryPruningAlgorithm.NONE;
	public double mainMemoryRandomSamplingRate = 0.1;		// what percent of rows to sample
	public int mainMemoryNumViewsToSelect = 10;				// how many top views to select
	public double mainMemoryUCB1Rho = 0.2;					// exploration parameter in UCB1
	public long mainMemoryNumRows = 50000;					// number of rows in the table
	public int mainMemoryNumPhases = 10;					// number of phases in which to run algorithm
	public boolean mainMemoryPhased = false;				// whether to execute algorithm in phases
	public int mainMemoryMinRows = 1000;				    // minimum # of rows to process before pruning
	public double mainMemoryCIMultiplier = 2;				// threshold for CI 
	public int mainMemoryMaxGroups = 30;					// maximum number of distinct values in any column
	public boolean MAB = false;								// use MAB for pruning
	
	//public String shared_buff="32MB";						

	
	/**
	 * Get default settings for SeeDB
	 * @return settings object
	 */
	public static ExperimentalSettings getDefault() {
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList(DifferenceOperators.AGGREGATE);
		return settings;
	}
	
	// returns a description of the settings used to run the SeeDB test
	public String getDescriptor() {
		List<String> l = Lists.newArrayList();
		l.add(backend == Backend.POSTGRES ? "row" : "column");
		l.add(useParallelExecution ? "parallel" + this.maxDBConnections : "seq");
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
		
		if (MAB) {
			l.add("MAB");
		}
		return Joiner.on("_").join(l);
	}
}
