package views;

import utils.Constants.AggregateFunctions;


public class AggregateValuesWrapper {
	public class AggregateValues {
		public double count;
		public double sum;
		public double average;
		
		public double getValue(AggregateFunctions func) {
			switch (func) {
			case COUNT:
				return count;
			case SUM:
				return sum;
			case AVG:
				return average;
			}
			return -1;
		}
	}
	public AggregateValues datasetValues[] = {new AggregateValues(), new AggregateValues()};
}
