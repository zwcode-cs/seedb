package main_memory_implementation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.Attribute;
import common.DifferenceQuery;
import common.GraphingUtils;
import common.Utils;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.DistanceMetric;
import utils.Constants.AggregateFunctions;
import views.AggregateGroupByView;
import views.View;

public class DistributionTest {
	public void processFile(String filename, int q_idx, String q_value, String[] dims_, 
			String[] measures_, final ExperimentalSettings settings) {
		Map<Integer, String> dim_idx = Maps.newHashMap(); // map from index to name of dimension column
		Map<Integer, String> measure_idx = Maps.newHashMap(); // map from index to name of measure column
		Map<String, View> views = Maps.newHashMap(); // map from view id to view
		Map<String, Integer> dim_idx_reverse = Maps.newHashMap(); // map from name of dimension column to idx
		Map<String, Integer> measure_idx_reverse = Maps.newHashMap(); // map from name of measure column to idx
		
		BufferedReader br;
		File logFile = null;
		Random r = new Random(System.nanoTime());
		
		try {
			
			br = new BufferedReader(new FileReader(filename));
			String line;
			boolean first = true;
			while ((line = br.readLine()) != null) {
			   List<String> dims = Lists.newArrayList();
			   List<String> measures = Lists.newArrayList();
			   if (first) {
				   // read in schema
				   String parts[] = line.split(",");
				   for (int i = 0 ; i < parts.length; i++) {
					   if (i == q_idx) {
						   continue;
					   }
					   String part = parts[i].trim();
					   if (part.startsWith("dim")) {
						   dims.add(part);
						   dim_idx.put(i, part);
						   dim_idx_reverse.put(part,  i);
					   } else if (part.startsWith("measure")) {
						   measures.add(part);
						   measure_idx.put(i, part);
						   measure_idx_reverse.put(part,  i);
					   }
				   }
				   
				   // create the specific views
				   for (int i = 0; i < dims_.length; i++) {
					   DifferenceQuery dq = new DifferenceQuery();
					   dq.groupByAttributes.add(new Attribute(dims_[i]));
					   dq.aggregateAttributes.add(new Attribute(measures_[i]));
					   views.put(dims_[i] + "__" + measures_[i], new AggregateGroupByView(dq));
				   }
					
				   first = false;
				   logFile = new File("/Users/manasi/Public/distribution_test3_" + settings.mainMemoryRandomSamplingRate + ".txt");
					if (!logFile.exists()) {
						logFile.createNewFile();
					}
				   continue;
			   }
			   
			   if (settings.mainMemoryRandomSample && r.nextDouble() > settings.mainMemoryRandomSamplingRate) {
			    	continue;
			   }
			   // read each line and populate views
			   String parts[] = line.split(",");
			   for (int i = 0; i < parts.length; i++) {
				   parts[i] = parts[i].trim();
			   }
			   
			
			   for (int i = 0; i < dims_.length; i++) {
				  String key = dims_[i] + "__" + measures_[i];
				  AggregateGroupByView view = (AggregateGroupByView) views.get(key);
				  if (parts[q_idx].equalsIgnoreCase(q_value)) { // check if the row satisfies the query
					  view.addAggregateValue(parts[dim_idx_reverse.get(dims_[i])], AggregateFunctions.COUNT, 1.0, 1);
					  view.addAggregateValue(parts[dim_idx_reverse.get(dims_[i])], AggregateFunctions.SUM, 
							  new Double(Double.parseDouble(parts[measure_idx_reverse.get(measures_[i])].trim())), 1);
				  } 
				  view.addAggregateValue(parts[dim_idx_reverse.get(dims_[i])], AggregateFunctions.COUNT, 1.0, 2);
				  view.addAggregateValue(parts[dim_idx_reverse.get(dims_[i])], AggregateFunctions.SUM, 
						  new Double(Double.parseDouble(parts[measure_idx_reverse.get(measures_[i])].trim())), 2); 
			   }
			}
			br.close();
			for (String key : views.keySet()) {
				Utils.writeToFile(logFile, key + ", " + views.get(key).getUtility(settings.distanceMetric));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}	
	}
	
	public static void main(String args[]) {
		DistributionTest dt = new DistributionTest();
		ExperimentalSettings es = new ExperimentalSettings();
		es.mainMemoryRandomSample = true;
		es.distanceMetric = DistanceMetric.KULLBACK_LEIBLER_DISTANCE;
		double[] fractions = {0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
		String filename = "/Users/manasi/Public/diabetes_50k.csv";
		int q_idx = 4;
		String q_value = "[30-40)";
		String[] dims_ = {"dim_gender", "dim_race", "dim_admission_type_id"};
		String[] measures_ = {"measure_number_emergency", "measure_number_diagnoses", "measure_num_medications"};
		for (double fraction : fractions) {
			fraction=0.1;
			for (int i = 0; i < 10000; i++) {
				es.mainMemoryRandomSamplingRate = fraction;
				dt.processFile(filename, q_idx, q_value, dims_, measures_, es);
			}
		}
	}
	
}
