package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import main_memory_implementation.MainMemorySeeDB;

import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import settings.DBSettings;
import settings.ExperimentalSettings;
import settings.ExperimentalSettings.Backend;
import settings.ExperimentalSettings.ComparisonType;
import settings.ExperimentalSettings.DifferenceOperators;
import views.AggregateGroupByView;
import views.AggregateValuesWrapper;
import views.AggregateValuesWrapper.AggregateValues;
import views.View;
import api.SeeDB;

import common.Utils;

/**
 * Test suite checking overall correctness of SeeDB
 * @author manasi
 *
 */
public class EndToEndTests {
	// we work with a single dataset
	private String table = "seedb_e2e_test";
	private String query1 = "select * from " + table + " where dim1_3='def'";
	private String s_1_query1 = "select * from s_1 where dim10_50='jm70ef'";
	private HashMap<String, HashMap<String, AggregateValuesWrapper>> expectedResults;
	private String viewOrder;
	private String utilityOrder;
	
	// populate expected results
	private void performSetup() {
		expectedResults = Maps.newHashMap();
		String key = "dim2_3__measure1";
		HashMap<String, AggregateValuesWrapper> value = Maps.newHashMap();
		AggregateValuesWrapper wrapper = new AggregateValuesWrapper();
		String localKey = "abc";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(1, 100, 100);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "def";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(3, 1700, 566.67);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(6, 3600, 600);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "ghi";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(3, 2400, 800);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
		
		key = "dim3_4__measure2";
		value = Maps.newHashMap();
		wrapper = new AggregateValuesWrapper();
		localKey = "abc";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(2, 5000, 2500);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(4, 12000, 3000);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "ghi";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(1, 7000, 7000);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(1, 7000, 7000);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
		wrapper = new AggregateValuesWrapper();
		localKey = "jkl";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(1, 1500, 1500);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
		wrapper = new AggregateValuesWrapper();wrapper = new AggregateValuesWrapper();
		localKey = "mno";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(4, 25000, 6250);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
		
		key = "dim2_3__measure4";
		value = Maps.newHashMap();
		wrapper = new AggregateValuesWrapper();
		localKey = "abc";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(1, 2500, 2500);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "def";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(3, 14000, 4666.67);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(6, 27750, 4625);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "ghi";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(3, 20500, 6833.33);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
		
		key = "dim4_4__measure3";
		value = Maps.newHashMap();
		wrapper = new AggregateValuesWrapper();
		localKey = "pqr";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(1, 300, 300);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "stu";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(1, 5000, 5000);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(2, 6000, 3000);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "vwx";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(2, 1700, 850);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(3, 2400, 800);
		value.put(localKey, wrapper);
		wrapper = new AggregateValuesWrapper();
		localKey = "yza";
		wrapper.datasetValues[0] = wrapper.new AggregateValues(0, 0, 0);
		wrapper.datasetValues[1] = wrapper.new AggregateValues(4, 6600, 1650);
		value.put(localKey, wrapper);
		expectedResults.put(key, value);
	}
	
	@Test
	public void allTest() {
		performSetup();
		
		// no optimization
		noOptimization();
		
		// main memory imeplementation
		mainMemoryTest();
		
		if (true) return;
		// parallel query execution
		noOptimizationParallel();
		
		// all systems optimizations, no temp tables
		allSystemOptimizationsParallel();
		
		// all systems optimizations with temp tables
		allSystemOptimizationsTempTablesParallel();
	}
	
	//@Test
	public void mainMemoryTest() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.backend = Backend.MAIN_MEMORY;
		settings.normalizeDistributions = false;
		runSeeDB("", settings, null);
	}
	
	//@Test
	public void createCharts() {
		performSetup();
		allSystemOptimizationsTempTablesParallel();
	}
	
	//@Test
	public void noOptimization() {
		performSetup();
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= true;
		settings.useTempTables = false;
		settings.useParallelExecution = false;
		settings.mergeQueries = false;
		settings.normalizeDistributions = false;
		runSeeDB(query1, settings, DBSettings.getLocalDefault());
	}
	
	private boolean checkCorrectness(List<View> result, String query) {
		for (View v : result) {
			if (v instanceof AggregateGroupByView) {
				AggregateGroupByView v_ = (AggregateGroupByView) v;
				String s = v_.getId();
				if (expectedResults.containsKey(v_.getId())) {
					HashMap<String, AggregateValuesWrapper> expected = expectedResults.get(v_.getId());
					HashMap<String, AggregateValuesWrapper> actual = v_.getResult();
					if (expected.keySet().size() != actual.keySet().size()) {
						System.out.println("Sizes of dictionaries is different");
						return false;
					}
					for (String key : expected.keySet()) {
						if (!actual.containsKey(key)) {
							System.out.println("Expected key: " + key + " not present in actual");
							return false;
						}
						if (!expected.get(key).equals(actual.get(key))) {
							return false;
						}
					}
				}
			}
		}
		return true;
	}
	
	private void runSeeDB(String query, ExperimentalSettings settings, DBSettings s) {
		long start 	= System.currentTimeMillis();
		
		List<View> result = null;
		if (settings.backend != Backend.MAIN_MEMORY) {
			try {
				SeeDB seedb = new SeeDB();
				seedb.connectToDatabase(s.database, s.databaseType, s.username, s.password);
				seedb.initialize(query, null, settings);
				result = seedb.computeDifference();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			} finally {
				File f = null;
				if (settings.logFile != null) {
					f = new File(settings.logFile);
				}
				Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
			}
		} else {
			MainMemorySeeDB seedb = new MainMemorySeeDB();
			String filename = "/Users/manasi/Documents/workspace/seedb/SeeDB-java-backend/src/test/test.txt";
			int q_idx = 0;
			String q_value = "def";
			result = seedb.processFile(filename, q_idx, q_value, settings);
		}
		
		assertTrue(checkCorrectness(result, query));
		List<String> tmp1 = Lists.newArrayList();
		List<String> tmp2 = Lists.newArrayList();
		for (View v : result) {
			AggregateGroupByView v_ = (AggregateGroupByView) v;
			tmp1.add(v_.getId());
			tmp2.add("" + v_.getUtility(settings.distanceMetric, settings.normalizeDistributions));
		}
		String localViewOrder =  Joiner.on(";").join(tmp1);
		String localUtilityOrder = Joiner.on(";").join(tmp2);
		if (this.viewOrder == null || this.viewOrder.isEmpty()) {
			// populate
			this.viewOrder = localViewOrder;
			this.utilityOrder = localUtilityOrder;
		} else {
			// check
			assertTrue(this.viewOrder.equals(localViewOrder) || this.utilityOrder.equals(localUtilityOrder));
		}
		
	}

	public void noOptimizationParallel() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= true;
		settings.useParallelExecution 			= true;
		settings.useTempTables 					= false;
		System.out.println("using postgres");
		runSeeDB(s_1_query1, settings, DBSettings.getPostgresDefault());
	}
	
	public void allSystemOptimizationsParallel() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= false;
		settings.optimizeAll 					= true;
		settings.combineMultipleAggregates 		= true;
		settings.maxAggSize 					= 3;
		settings.useBinPacking 					= false;
		settings.useHeuristic 					= false;
		settings.useParallelExecution 			= true;
		settings.useTempTables 					= false;
		settings.maxGroupBySize 				= 3;
		runSeeDB(s_1_query1, settings, DBSettings.getPostgresDefault());
	}
	
	public void allSystemOptimizationsTempTablesParallel() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= false;
		settings.optimizeAll 					= true;
		settings.combineMultipleAggregates 		= true;
		settings.maxAggSize 					= 5;
		settings.useBinPacking 					= false;
		settings.useHeuristic 					= false;
		settings.useParallelExecution 			= true;
		settings.useTempTables 					= true;
		settings.maxGroupBySize 				= 3;
		//settings.useHeuristic					= true;
		settings.maxDBConnections				= 4;
		runSeeDB(s_1_query1, settings, DBSettings.getVerticaDefault());
	}
}
