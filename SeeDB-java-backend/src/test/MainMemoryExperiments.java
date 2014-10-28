package test;

import static org.junit.Assert.*;

import java.util.List;

import main_memory_implementation.MainMemorySeeDB;

import org.junit.Test;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.Backend;
import settings.ExperimentalSettings.ComparisonType;
import settings.ExperimentalSettings.DifferenceOperators;
import views.AggregateGroupByView;
import views.View;

import com.google.common.collect.Lists;

public class MainMemoryExperiments {
	int ks[] = {5, 10, 20, 40};
	int percentages[] = {10, 20, 30, 50, 70, 80, 90};
	String datasets[] = {"/Users/manasi/Public/diabetes_50k.csv", 
			//"/Users/manasi/Documents/workspace/seedb/generator/table_1000000_100_1_1_data.txt"
			"/Users/manasi/Public/seedb_results/data/m_1_data.txt",
			"/Users/manasi/Downloads/bank-additional/bank-additional-full.csv",
			"/Users/manasi/Public/seedb_results/data/s_1_data.txt"
			};
	int q_idxs[] = {4, 3, 3, 6};
	String q_values[] = {"[30-40)", "p8rmu9", "high.school", "o11hem"};
	
	int sizes[] = {50000, 1000000, 41189, 1000000};
	
	/**
	 * to run on different datasets: generated one, diabetes
	 * to run for various ks: 5, 10, 20, 50
	 */
	//@Test
	public void pruningBasicTest() {
		
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.backend = Backend.MAIN_MEMORY;
		settings.normalizeDistributions = true;
		//settings.makeGraphs = true;
		
		List<View> result = null;
		List<View> tempResult = null;
		MainMemorySeeDB seedb = new MainMemorySeeDB();
		
		for (int dataset_idx = 0; dataset_idx < 1; dataset_idx++) {
			// 1. run for entire dataset
			settings.num_rows = sizes[dataset_idx];
			result = seedb.processFile(datasets[dataset_idx], q_idxs[dataset_idx], 
					q_values[dataset_idx], settings);
			if (settings.makeGraphs) {
				continue;
			}
			
			for (int percentage : percentages) {
				// 2. rerun, for every given percentage, get top k and compare how many are missing
				settings.num_rows = (int) Math.ceil(percentage * sizes[dataset_idx] * 1.0 / 100);
				tempResult = seedb.processFile(datasets[dataset_idx], q_idxs[dataset_idx], 
						q_values[dataset_idx], settings);
				
				for (int k : ks) {
					if (k > result.size()) {
						continue;
					}
					
					for (int kk : ks) {
						if (kk < k || kk > result.size()) {
							continue;
						}
						int matches = 0;
						// check each item in the top k for overlaps
						for (int ii = 0; ii < k; ii++) {
							String query = ((AggregateGroupByView) result.get(ii)).getId();
							double query_utility = ((AggregateGroupByView) result.get(ii)).getUtility(settings.distanceMetric);
							for (int j = 0; j < kk; j++) {
								String data = ((AggregateGroupByView) tempResult.get(j)).getId();
								double data_utility = ((AggregateGroupByView) tempResult.get(ii)).getUtility(settings.distanceMetric);
								if (data.equalsIgnoreCase(query)) {
									matches++;
									//System.out.println(query + "," + data + "," + query_utility + "," + data_utility);
								}
							}
						}
						System.out.println(percentage + "," + k + "," + kk + "," + matches);	
					} // end kk : ks
				} // end k : ks
			}
		}
	}
	
	@Test
	public void distributionTest() {
		double sampleSize = 0.01;
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.backend = Backend.MAIN_MEMORY;
		settings.normalizeDistributions = true;
		settings.mainMemoryRandomSample = true;
		//settings.makeGraphs = true;
		
		List<View> result = null;
		MainMemorySeeDB seedb = new MainMemorySeeDB();
		
		for (int dataset_idx = 3; dataset_idx < 4; dataset_idx++) {
			for (double i = sampleSize; i <= 1; i+= sampleSize) {
				// 2. rerun, for every given percentage, get top k and compare how many are missing
				settings.mainMemoryRandomSamplingRate = i;
				result = seedb.processFile(datasets[dataset_idx], q_idxs[dataset_idx], 
						q_values[dataset_idx], settings);
			}
		}
	}
}
