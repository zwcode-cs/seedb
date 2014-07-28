package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.junit.Test;

import views.View;
import api.SeeDB;

import com.google.common.collect.Lists;
import common.ExperimentalSettings;
import common.Utils;
import common.ExperimentalSettings.ComparisonType;
import common.ExperimentalSettings.DifferenceOperators;

public class AdityaTests {

	private String small1 	= "select * from s where dim10_50='jm70ef'";
	private String small2 	= "select * from s where dim1_100='35mtw7'";
	private String small3 	= "select * from s where dim1_1000='vn2dp6'";
	private String med1 	= "select * from m where dim1_10 ='b3ckzj'";
	private String med2 	= "select * from m where dim15_50='adk7cz'";
	private String med3 	= "select * from m where dim1_100='9ecz4e'";
	private String large1 	= "select * from l where dim3_50='8g9oat'";
	private String large2 	= "select * from l where dim3_50='rc7jz5'";
	private String large3 	= "select * from l where dim7_50='c1s4u3'";
	
	private String[] small_queries 	= new String[]	{small1, 	small2, 	small3};
	private String[] medium_queries = new String[] 	{med1, 		med2, 		med3};
	private String[] large_queries 	= new String[] 	{large1, 	large2, 	large3};
	
	private String[][] queries 		= new String[][]{small_queries, medium_queries, large_queries};

	private String[] tables 		= new String[]	{small, med, large};

	private int[] nconns 	= new int[]{2, 5, 10, 15, 20, 25, 30, 35, 40};
	private int[] ndistinct = new int[]{1, 3, 5, 10, 20, 25, 40, 50, 75, 100};

	private int[] agg_sizes = new int[]{10, 10, 10}; 

	@Test
	public void allTestsNew() {

		for (int k = 0; k < tables.length; k ++) {
			System.out.println("Table:" + tables[k]);

			for (int i = 0; i < queries[k].length; i ++) {
				System.out.println(queries[k][i]);

				for (int j = 0; j < nconns.length; j ++ ) {
					System.out.println("NCONNS:" + nconns[j]);

					for (int l = 0; l < ndistinct.length; l ++ ){
						System.out.println("NGB:" + ndistinct[l]);

						for (int ii = 0; ii <3; ii++) {
							endToEndAggregateGroupByDifferenceParallelAllOpTest(queries[k][i], tables[k], ndistinct[l], agg_sizes[k], nconns[j]);
						}
					}
				}
			}
		}
	}


	public void endToEndAggregateGroupByDifferenceParallelAllOpTest(String query, String table, int nGB, int maxAggSize, int nconns) {
		
		long start 	= System.currentTimeMillis();
		SeeDB seedb = new SeeDB();

		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		
		settings.differenceOperators 			= Lists.newArrayList();

		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		
		settings.noAggregateQueryOptimization 	= false;
		settings.optimizeAll 					= true;

		settings.combineMultipleAggregates 		= true;
		settings.maxAggSize 					= maxAggSize; // max aggregate size

		settings.useBinPacking 					= false;
		settings.useHeuristic 					= false;

		settings.useParallelExecution 			= true;
		settings.maxDBConnections 				= nconns;
		settings.useTempTables 					= true;

		settings.maxGroupBySize 				= nGB;
		
		settings.logFile 						= "testResults/" + settings.getDescriptor() + "_" + table;

		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
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
		



		/*
		long start = System.currentTimeMillis();

		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;

		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);

		settings.noAggregateQueryOptimization = false;
		settings.optimizeAll = true;

		settings.combineMultipleAggregates = true;
		settings.maxAggSize = 5; // max aggregate size

		settings.useBinPacking = false;
		settings.useHeuristic = true;

		settings.mergeQueries = true;

		settings.useParallelExecution = true;
		settings.useTempTables = true;
		settings.maxDBConnections = 40;

		settings.logFile = "testResults/" + settings.getDescriptor() + "_" + table;

		try {
			seedb.initialize("", null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
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
		*/



	

}
