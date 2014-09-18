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
	public double getUtility(DistanceMetric distanceMetric) {
		populateAvg();
		switch (distanceMetric) {
		case EARTH_MOVER_DISTANCE:
			return UtilityMetrics.EarthMoverDistance(this.aggregateValues);
		case EUCLIDEAN_DISTANCE:
			return UtilityMetrics.EuclideanDistance(this.aggregateValues);
		case COSINE_DISTANCE:
			return UtilityMetrics.CosineDistance(this.aggregateValues);
		case FIDELITY_DISTANCE:
			return UtilityMetrics.FidelityDistance(this.aggregateValues);
		case CHI_SQUARED_DISTANCE:
			return UtilityMetrics.ChiSquaredDistance(this.aggregateValues);
		case KULLBACK_LEIBLER_DISTANCE:
			return UtilityMetrics.EntropyDistance(this.aggregateValues);
		}
		return -1;
	}
	
	public String getId() {
		return this.groupByAttribute + Constants.spacer + Constants.spacer + this.aggregateAttribute;
	}
}
