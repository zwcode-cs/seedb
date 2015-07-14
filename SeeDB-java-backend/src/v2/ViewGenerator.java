package v2;

import java.util.ArrayList;

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
	public void generateViewStubs(
			DBMetadata metadata, 
			InputQuery targetQuery,
			InputQuery referenceQuery, 
			UserPreferences preferences, 
			Setting setting) {
		// for single viz
		
		// for comparative viz
		// for manual
		
		// for recommendations
		// go through DB metadata and identify dimensions and measures 
		// TODO: not in query
		// take cross product
		
		ArrayList<AggregateComparisonView> views 
			= new ArrayList<AggregateComparisonView>();
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
		
	}

}
