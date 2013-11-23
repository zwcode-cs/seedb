package utils;

import java.util.ArrayList;

public class UtilityMetrics {
	
	public static final int DISTRIBUTION_EMPTY = -1;
	
	/**
	 * Compute the earth mover distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EarthMoverDistance(ArrayList<DistributionUnit> queryDistribution, 
			ArrayList<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				utility += Math.abs(queryDistribution.get(query_idx).fraction - datasetDistribution.get(dataset_idx).fraction);
				query_idx += 1;
			} else {
				utility += datasetDistribution.get(dataset_idx).fraction;
			}
		}
		utility /= 2;
		return utility;
	}
}