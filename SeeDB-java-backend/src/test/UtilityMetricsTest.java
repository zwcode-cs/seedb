package test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

public class UtilityMetricsTest {
	public static final double assertEqualEpsilon = 1E-10;

	/*
	public void TestEarthMoverDistance() {
		ArrayList<DistributionUnit> queryDistribution = new ArrayList<DistributionUnit>();
		ArrayList<DistributionUnit> datasetDistribution = new ArrayList<DistributionUnit>();
		assertEquals(UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution), 
				UtilityMetrics.DISTRIBUTION_EMPTY, assertEqualEpsilon);
		
		datasetDistribution.add(new DistributionUnit("January", 0.36));
		datasetDistribution.add(new DistributionUnit("March", 0.22));
		datasetDistribution.add(new DistributionUnit("August", 0.17));
		datasetDistribution.add(new DistributionUnit("November", 0.25));
		
		assertEquals(UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution), 
				UtilityMetrics.DISTRIBUTION_EMPTY, assertEqualEpsilon);
		
		queryDistribution.add(new DistributionUnit("August", 1));
		assertEquals(UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution), 
				0.83, assertEqualEpsilon);
		
		queryDistribution.clear();
		queryDistribution.add(new DistributionUnit("March", 0.75));
		queryDistribution.add(new DistributionUnit("November", 0.25));
		assertEquals(UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution), 
				0.53, assertEqualEpsilon);
		
		queryDistribution.clear();
		queryDistribution.add(new DistributionUnit("January", 0.18));
		queryDistribution.add(new DistributionUnit("March", 0.67));
		queryDistribution.add(new DistributionUnit("August", 0.01));
		queryDistribution.add(new DistributionUnit("November", 0.14));
		assertEquals(UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution), 
				0.45, assertEqualEpsilon);
	}
	*/

}
