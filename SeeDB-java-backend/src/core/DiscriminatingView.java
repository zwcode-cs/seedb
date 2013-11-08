package core;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import utils.DistributionUnit;
import utils.UtilityMetrics;

public class DiscriminatingView {
	private String aggregateAttribute;
	private String groupByAttribute;
	private ArrayList<DistributionUnit> queryDistribution;
	private ArrayList<DistributionUnit> datasetDistribution;
	private double viewUtility;
	
	public DiscriminatingView(String aggregateAttribute, String groupByAttribute, 
			ArrayList<DistributionUnit> queryDistribution, ArrayList<DistributionUnit> datasetDistribution)
	{
		this.aggregateAttribute = aggregateAttribute;
		this.groupByAttribute = groupByAttribute;	
		this.queryDistribution = queryDistribution;
		this.datasetDistribution = datasetDistribution;
		viewUtility = 0;
	}
	
	public ArrayList<DistributionUnit> getQueryDistribution() {
		return this.queryDistribution;
	}
	
	public ArrayList<DistributionUnit> getDatasetDistribution() {
		return this.datasetDistribution;
	}
	
	// this is mainly for python
	public ArrayList<String> getCombinedDistribution() {
		ArrayList<String> result = new ArrayList<String>();
		Hashtable<String, double[]> h = new Hashtable<String, double[]>();
		for (DistributionUnit d: datasetDistribution) {
			h.put(d.attributeValue.toString(), new double[]{d.fraction, 0});	
		}
		for (DistributionUnit d: queryDistribution) {
			h.get(d.attributeValue.toString())[1] = d.fraction;	
		}
		Enumeration<String> e = h.keys();
		while (e.hasMoreElements()) {
			String key = e.nextElement();
			result.add(key + ":" + h.get(key)[0] + ":" + h.get(key)[1]);
		}
		return result;
	}
	
	public void computeUtility()
	{
		this.viewUtility = UtilityMetrics.EarthMoverDistance(queryDistribution, datasetDistribution);
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
