package core;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import utils.DistributionUnit;
import utils.UtilityMetrics;

public class DiscriminatingViewTest {
	public static final double assertEqualEpsilon = 1E-10;

	@Test
	public void ComputeUtilityTest() {
		String aggregateAttribute = "aggregateAttribute";
		String groupByAttribute = "groupByAttribute";
		
		ArrayList<DistributionUnit> queryDistribution = new ArrayList<DistributionUnit>();
		ArrayList<DistributionUnit> datasetDistribution = new ArrayList<DistributionUnit>();
		datasetDistribution.add(new DistributionUnit("January", 0.36));
		datasetDistribution.add(new DistributionUnit("March", 0.22));
		datasetDistribution.add(new DistributionUnit("August", 0.17));
		datasetDistribution.add(new DistributionUnit("November", 0.25));
		
		queryDistribution.add(new DistributionUnit("January", 0.18));
		queryDistribution.add(new DistributionUnit("March", 0.67));
		queryDistribution.add(new DistributionUnit("August", 0.01));
		queryDistribution.add(new DistributionUnit("November", 0.14));
						
		DiscriminatingView view = new DiscriminatingView(aggregateAttribute, groupByAttribute, 
				queryDistribution, datasetDistribution);
		view.computeUtility();
		assertEquals(view.getUtility(), 0.45, assertEqualEpsilon);
	}

}
