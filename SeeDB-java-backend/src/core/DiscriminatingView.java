package core;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import utils.DistributionUnit;
import utils.UtilityMetrics;

/**
 * This class stores all the information about a discriminating view and performs computations
 * on the view. Specifically, it store sthe groupby and aggregate attributes, distributions for
 * the query and full dataset and utility. It has functions to compute utility based on various 
 * metrics
 * 
 * @author manasi
 * 
 */
public class DiscriminatingView {
	private String aggregateAttribute;
	private String groupByAttribute;
	private ArrayList<DistributionUnit> queryDistribution;
	private ArrayList<DistributionUnit> datasetDistribution;
	private Hashtable<String, double[]> combinedDistribution;
	private ArrayList<String> combinedDistributionAsStrings;
	
	private double viewUtility;
	
	public DiscriminatingView(String aggregateAttribute, String groupByAttribute, 
			ArrayList<DistributionUnit> queryDistribution, ArrayList<DistributionUnit> datasetDistribution)
	{
		this.aggregateAttribute = aggregateAttribute;
		this.groupByAttribute = groupByAttribute;	
		this.queryDistribution = queryDistribution;
		this.datasetDistribution = datasetDistribution;
		viewUtility = 0;
		
		// make both distributions uniform (i.e. same values)
		combinedDistribution = new Hashtable<String, double[]>();
		
		// populate the distributions for the entire dataset, placeholder for query distribution
		for (DistributionUnit d: datasetDistribution) {
			combinedDistribution.put(d.attributeValue.toString(), new double[]{d.fraction, 0});	
		}
		
		// update query distribution
		for (DistributionUnit d: queryDistribution) {
			if (!combinedDistribution.containsKey(d.attributeValue)) {
				combinedDistribution.put(d.attributeValue.toString(), new double[]{0, d.fraction});
			} else {
				combinedDistribution.get(d.attributeValue.toString())[1] = d.fraction;	
			}
		}
		
		datasetDistribution.clear();
		queryDistribution.clear();
		
		ArrayList<String> result = new ArrayList<String>();
		Enumeration<String> e = this.combinedDistribution.keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			result.add(key + ":" + this.combinedDistribution.get(key)[0] + ":" + 
					this.combinedDistribution.get(key)[1]);
			datasetDistribution.add(new DistributionUnit(key, combinedDistribution.get(key)[0]));
			queryDistribution.add(new DistributionUnit(key, combinedDistribution.get(key)[1]));
		}
		this.combinedDistributionAsStrings = result;		
	}
	
	public ArrayList<DistributionUnit> getQueryDistribution() {
		return this.queryDistribution;
	}
	
	public ArrayList<DistributionUnit> getDatasetDistribution() {
		return this.datasetDistribution;
	}
	
	/**
	 * This function returns a list of strings which has the attributevalue followed by fraction
	 * in entire dataset and fraction in query. Example return value:
	 * ["ABC:0.5:0.01", "DEF:0.33:0.21"]
	 * 
	 */
	public ArrayList<String> getCombinedDistribution() {
		// format as strings
		return combinedDistributionAsStrings;
	}
	
	/**
	 * Computes the utility of this view based on the metric specified
	 * @param metric String name of metric to be used
	 */
	public void computeUtility(String metric)
	{
		if (metric.equals("EarthMoverDistance")) {
			this.viewUtility = UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution);
		} else if (metric.equals("EuclideanDistance")) {
			this.viewUtility = UtilityMetrics.EuclideanDistance(queryDistribution, datasetDistribution);
		} else if (metric.equals("CosineDistance")) {
			this.viewUtility = UtilityMetrics.CosineDistance(queryDistribution, datasetDistribution);
		} else if (metric.equals("FidelityDistance")) {
			this.viewUtility = UtilityMetrics.FidelityDistance(queryDistribution, datasetDistribution);
		} else if (metric.equals("ChiSquaredDistance")) {
			this.viewUtility = UtilityMetrics.ChiSquaredDistance(queryDistribution, datasetDistribution);
		}
	}
	
	public double getUtility()
	{
		return viewUtility;
	}
	
	public String getAggregateAttribute()
	{
		return aggregateAttribute;
	}
	
	public String getGroupByAttribute()
	{
		return groupByAttribute;
	}
}
