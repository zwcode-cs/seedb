package v2;

import java.util.ArrayList;

public class AggregateComparisonView extends View {
	public static enum AggregateFunctions {SUM, COUNT, AVG}; 
	// TODO: support other e.g. std dev
	// TODO: some of our techniques wont work for std dev etc.
	private Attribute dimension;
	private Attribute measure;
	private ArrayList<AggregateFunctions> functions;
	
	public AggregateComparisonView(Attribute dimension, Attribute measure) {
		this.isComparative = true;
		this.isAggregate = true;
		this.dimension = dimension;
		this.measure = measure;
		this.functions = new ArrayList<AggregateFunctions>();
		this.functions.add(AggregateFunctions.COUNT);
		this.functions.add(AggregateFunctions.SUM); // for avg, we still need these
	}
	
	public String toString() {
		return this.dimension + ";" + this.measure;
	}
}
