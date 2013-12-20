package utils;

import java.util.ArrayList;
import java.util.List;

public class UtilityMetrics {
	
	public static final int DISTRIBUTION_EMPTY = -1;
	
	/**
	 * Compute the earth mover distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EarthMoverDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
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
	
	
	/**
	 * Compute the euclidean distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EuclideanDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				utility += Math.pow(Math.abs(queryDistribution.get(query_idx).fraction - datasetDistribution.get(dataset_idx).fraction), 2);
				query_idx += 1;
			} else {
				utility += Math.pow(datasetDistribution.get(dataset_idx).fraction , 2);
			}
		}
		utility = Math.sqrt(utility);
		return utility;
	}
	
	/**
	 * Compute the cosine distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double CosineDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		double similarity = 0;
		double numerator = 0;
		double queryInDenominator = 0;
		double datasetInDenominator = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				numerator += Math.abs(queryDistribution.get(query_idx).fraction * datasetDistribution.get(dataset_idx).fraction);
				queryInDenominator += Math.pow(queryDistribution.get(query_idx).fraction, 2);
				datasetInDenominator += Math.pow(datasetDistribution.get(dataset_idx).fraction, 2);
				query_idx += 1;
			} else {
				datasetInDenominator += Math.pow(datasetDistribution.get(dataset_idx).fraction, 2);
			}
		}
		similarity = numerator/(queryInDenominator * datasetInDenominator);
		utility = -similarity;
		return utility;
	}
	
	/**
	 * Compute the fidelity distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double FidelityDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				utility += Math.pow(Math.abs(queryDistribution.get(query_idx).fraction * datasetDistribution.get(dataset_idx).fraction), 1/2);
				query_idx += 1;
			}
		}
		utility = -1 * Math.log(Math.sqrt(utility));
		return utility;
	}
	
	/**
	 * Compute the pearson chi square distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double ChiSquaredDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		double denominator = 0;
		double numerator = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				numerator = Math.pow(queryDistribution.get(query_idx).fraction - datasetDistribution.get(dataset_idx).fraction, 2);
				denominator = Math.abs(datasetDistribution.get(dataset_idx).fraction);
				utility += numerator/denominator;
				query_idx += 1;
			}
		}
		return utility;
	}
	
	/**
	 * Compute the Kullback-Leibler Entropy distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EntropyDistance(List<DistributionUnit> queryDistribution, 
			List<DistributionUnit> datasetDistribution) {
		if ((queryDistribution.size() == 0) || (queryDistribution.size() == 0)) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		int query_idx = 0; // index into distributionForQuery
		int dataset_idx = 0; // index into distributionForDataset
		for (dataset_idx = 0; dataset_idx < datasetDistribution.size(); dataset_idx++) {
			if ((query_idx < queryDistribution.size()) && 
				(queryDistribution.get(query_idx).attributeValue.equals(datasetDistribution.get(dataset_idx).attributeValue))){
				utility += Math.log(Math.abs(queryDistribution.get(query_idx).fraction / datasetDistribution.get(dataset_idx).fraction)) * queryDistribution.get(query_idx).fraction;
				query_idx += 1;
			}
		}
		return utility;
	}
}
