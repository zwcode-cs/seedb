package views;

import java.util.List;

import settings.ExperimentalSettings.DifferenceOperators;
import settings.ExperimentalSettings.DistanceMetric;

/**
 * Defines a view, i.e. a particular type of summary of the two datasets
 * @author manasi
 *
 */
public interface View {
	
	public DifferenceOperators getOperator();
	public double getUtility(DistanceMetric distanceMetric);
	public double getUtility(DistanceMetric distanceMetric,
			boolean normalizeDistributions);
	public List<View> constituentViews();
	//public View getTopDifferent();

}
