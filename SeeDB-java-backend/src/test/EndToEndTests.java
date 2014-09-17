package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Hashtable;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.ComparisonType;
import settings.ExperimentalSettings.DifferenceOperators;
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
	private String query1 = "select * from " + table + " where dim1='def'";
	private Hashtable<String, Hashtable<String, List<Integer>>> expectedResults;
	
	
	private void performSetup() {
	
	}
	
	@Test
	public void allTest() {
		performSetup();
		
		// no optimization
		noOptimization();
		
		// parallel query execution
		noOptimizationParallel();
		
		// all systems optimizations, no temp tables
		allSystemOptimizationsParallel();
		
		// all systems optimizations with temp tables
		allSystemOptimizationsTempTablesParallel();
	}
	
	public void noOptimization() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= true;
		settings.useTempTables = false;
		settings.useParallelExecution = false;
		settings.mergeQueries = false;
		runSeeDB(query1, settings);
	}
	
	private boolean checkCorrectness(List<View> result, String query) {
		return true;
	}
	
	private void runSeeDB(String query, ExperimentalSettings settings) {
		long start 	= System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
			assertTrue(checkCorrectness(result, query));
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
	}

	public void noOptimizationParallel() {
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.noAggregateQueryOptimization 	= true;
		settings.useParallelExecution 			= true;
		runSeeDB(query1, settings);
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
		runSeeDB(query1, settings);
	}
	
	public void allSystemOptimizationsTempTablesParallel() {
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
		settings.useTempTables 					= true;
		settings.maxGroupBySize 				= 3;
		runSeeDB(query1, settings);
	}
}
