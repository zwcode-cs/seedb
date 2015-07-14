package v2;

import java.util.ArrayList;

import v2.Setting.VizSource;

public class ViewGenerator {
	
	/**
	 * Generate stubs for each view that must be evaluated. The list of views
	 * is based on the database metadata, input query, preferences and settings
	 * @param metadata
	 * @param inputQuery
	 * @param preferences
	 * @param setting
	 */
	// question about where viz type setting goes
	// also decide if input query goes into setting
	public static ArrayList<View> generateViewStubs(
			DBMetadata metadata, 
			InputQuery targetQuery,
			InputQuery referenceQuery, 
			InvocationParameters params,
			Setting setting) {
		if (params.vizSource == VizSource.MANUAL) {
			return null;
		} else { // recommendations
			if (params.comparativeVisualization) {
				ArrayList<View> views 
					= new ArrayList<View>();
				ArrayList<Attribute> dimensions = new ArrayList<Attribute>();
				ArrayList<Attribute> measures = new ArrayList<Attribute>();
				
				for (Attribute a : metadata.getAttributes()) {
					// TODO: if a in targetQuery or referenceQuery, continue
					if (a.isDimension()) {
						dimensions.add(a);
					} else if (a.isMeasure()) {
						measures.add(a);
					}
				}
				
				// consider all combinations
				for (Attribute d : dimensions) {
					for (Attribute m : measures) {
						views.add(new AggregateComparisonView(d, m));
					}
				}
				return views;
			} else {
				return null;
			}
		}	
	}
}
