package utils;

import java.util.HashMap;
import java.util.List;

import utils.Constants.AggregateFunctions;
import views.AggregateValuesWrapper;

public class UtilityMetrics {
	
	public static final int DISTRIBUTION_EMPTY = -1;
	
	/**
	 * Compute the earth mover distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EarthMoverDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		
		double utility = 0;
		for (String key : dist.keySet()) {
			utility += Math.abs(dist.get(key).datasetValues[0].getValue(func) - dist.get(key).datasetValues[1].getValue(func));
		}

		utility /= 2;
		return utility;
	}
	
	public static double EarthMoverDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return EarthMoverDistance(dist, AggregateFunctions.SUM);
	}
	
	
	/**
	 * Compute the euclidean distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EuclideanDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		
		double utility = 0;
		for (String key : dist.keySet()) {
			utility += Math.pow(dist.get(key).datasetValues[0].getValue(func) - 
									dist.get(key).datasetValues[1].getValue(func), 
								2);
		}
		utility = Math.sqrt(utility);
		return utility;
	}
	
	public static double EuclideanDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return EuclideanDistance(dist, AggregateFunctions.SUM);
	}
	
	/**
	 * Compute the cosine distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double CosineDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		
		double utility = 0;
		double numerator = 0;
		double queryInDenominator = 0;
		double datasetInDenominator = 0;
		
		for (String key : dist.keySet()) {
			numerator += Math.abs(dist.get(key).datasetValues[0].getValue(func) * dist.get(key).datasetValues[1].getValue(func));
			queryInDenominator += Math.pow(dist.get(key).datasetValues[0].getValue(func), 2);
			datasetInDenominator += Math.pow(dist.get(key).datasetValues[1].getValue(func), 2);
		}
		utility = numerator/(queryInDenominator * datasetInDenominator);
		return utility;
	}
	
	public static double CosineDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return CosineDistance(dist, AggregateFunctions.SUM);
	}
	
	/**
	 * Compute the fidelity distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double FidelityDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		
		for (String key : dist.keySet()) {
			utility += Math.pow(dist.get(key).datasetValues[0].getValue(func) * dist.get(key).datasetValues[1].getValue(func), 1/2);
		}
		utility = -1 * Math.log(Math.sqrt(utility));
		return utility;
	}
	
	public static double FidelityDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return FidelityDistance(dist, AggregateFunctions.SUM);
	}
	
	/**
	 * Compute the pearson chi square distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double ChiSquaredDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		double denominator = 0;
		double numerator = 0;
		
		for (String key : dist.keySet()) {
			numerator = Math.pow(dist.get(key).datasetValues[0].getValue(func) - dist.get(key).datasetValues[1].getValue(func), 2);
			denominator = Math.abs(dist.get(key).datasetValues[1].getValue(func));
			utility += numerator/denominator;
		}
		return utility;
	}
	
	public static double ChiSquaredDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return ChiSquaredDistance(dist, AggregateFunctions.SUM);
	}
	
	/**
	 * Compute the Kullback-Leibler Entropy distance between the distribution generate from the query and the distribution from the entire dataset
	 * @param distributionForQuery
	 * @param distributionForDataset
	 * @return
	 */
	public static double EntropyDistance(HashMap<String, AggregateValuesWrapper> dist, AggregateFunctions func) {
		if (dist.isEmpty()) {
			return DISTRIBUTION_EMPTY;
		}
		double utility = 0;
		for (String key : dist.keySet()) {
			utility += Math.log(Math.abs(dist.get(key).datasetValues[0].getValue(func)
					- dist.get(key).datasetValues[1].getValue(func)))
					* dist.get(key).datasetValues[0].getValue(func);
		}
		return utility;
	}
	
	public static double EntropyDistance(HashMap<String, AggregateValuesWrapper> dist) {
		return EntropyDistance(dist, AggregateFunctions.SUM);
	}
}
