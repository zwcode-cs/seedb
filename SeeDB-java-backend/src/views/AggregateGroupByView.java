package views;

import java.util.HashMap;

import settings.ExperimentalSettings.DifferenceOperators;
import settings.ExperimentalSettings.DistanceMetric;
import utils.Constants;
import utils.UtilityMetrics;
import views.AggregateValuesWrapper.AggregateValues;
import common.DifferenceQuery;

public class AggregateGroupByView extends AggregateView {

	private boolean populatedAvg = false;
	public AggregateGroupByView(DifferenceQuery dq) {
		super(dq);
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.AGGREGATE;
	}

	public String getGroupByAttributes() {
		return this.groupByAttribute;
	}
	
	public void populateAvg() {
		if (populatedAvg) {
			return;
		}
		for (String key : this.aggregateValues.keySet()) {
			AggregateValuesWrapper wrapper = aggregateValues.get(key);
			for (int i = 0; i < 2; i++) {
				if (wrapper.datasetValues[i].count != 0) {
					wrapper.datasetValues[i].average = wrapper.datasetValues[i].sum / wrapper.datasetValues[i].count;
				}
			}
		}
		populatedAvg = true;
	}

	public HashMap<String, AggregateValuesWrapper> getResult() {
		populateAvg();
		return this.aggregateValues;
	}
	
	public String toString() {
		populateAvg();
		String ret = "";
		ret += "diff_type:aggregate_diff;";
		ret += "aggregateValues:" + this.aggregateAttribute + ";";
		ret += "groupByValues:" + this.groupByAttribute + ";";
		ret += "data:[[";
		for (String key: this.aggregateValues.keySet()) {
			ret += key + ":";
			for (int i = 0; i < 2; i++) {
				AggregateValues tmp = aggregateValues.get(key).datasetValues[i];
				ret += tmp.count + "," + tmp.sum + "," + tmp.average + ";";
			}
		}
		ret += "]]";
		return ret;
	}

	@Override
	public double getUtility(DistanceMetric distanceMetric, boolean normalize) {
		if (normalize) {
			normalize();
		}
		populateAvg();
		switch (distanceMetric) {
		case EARTH_MOVER_DISTANCE:
			return UtilityMetrics.EarthMoverDistance(this.aggregateValues, normalize);
		case EUCLIDEAN_DISTANCE:
			return UtilityMetrics.EuclideanDistance(this.aggregateValues, normalize);
		case COSINE_DISTANCE:
			return UtilityMetrics.CosineDistance(this.aggregateValues, normalize);
		case FIDELITY_DISTANCE:
			return UtilityMetrics.FidelityDistance(this.aggregateValues, normalize);
		case CHI_SQUARED_DISTANCE:
			return UtilityMetrics.ChiSquaredDistance(this.aggregateValues, normalize);
		case KULLBACK_LEIBLER_DISTANCE:
			return UtilityMetrics.EntropyDistance(this.aggregateValues, normalize);
		}
		return -1;
	}
	
	@Override
	public double getUtility(DistanceMetric distanceMetric) {
		return getUtility(distanceMetric, true);
	}
	
	private void normalize() {
		double totalSum[] = {0, 0};
		double totalCount[] = {0, 0};
		double totalAverage[] = {0, 0};
		
		for (String key : this.aggregateValues.keySet()) {
			AggregateValuesWrapper wrapper = aggregateValues.get(key);
			for (int i = 0; i < 2; i++) {
				totalSum[i] += wrapper.datasetValues[i].sum;
				totalCount[i] += wrapper.datasetValues[i].count;
				totalAverage[i] += wrapper.datasetValues[i].average;
			}
		}
		for (String key : this.aggregateValues.keySet()) {
			AggregateValuesWrapper wrapper = aggregateValues.get(key);
			for (int i = 0; i < 2; i++) {
				if (totalSum[i] > 0) {
					wrapper.datasetValues[i].sumNormalized = wrapper.datasetValues[i].sum / totalSum[i];
				}
				if (totalCount[i] > 0) {
					wrapper.datasetValues[i].countNormalized = wrapper.datasetValues[i].count / totalCount[i];
				}
				if (totalAverage[i] > 0) {
					wrapper.datasetValues[i].averageNormalized = wrapper.datasetValues[i].average / totalAverage[i];
				}
			}
		}
	}

	public String getId() {
		return this.groupByAttribute + Constants.spacer + Constants.spacer + this.aggregateAttribute;
	}
}
