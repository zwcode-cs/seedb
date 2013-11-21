package utils;

public class RuntimeSettings {
	public int numViews = 10;
	public boolean useSampling = false;
	public double samplePercent = 1;
	public String metric = "EarthMoverDistance";
	public boolean usePruning = false;
	public boolean useIncrementalAggregateComputation = false;
	public boolean useMultipleAggregateSingleGroupByOptimization = false;
}
