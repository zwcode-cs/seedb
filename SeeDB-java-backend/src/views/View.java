package views;

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

}
