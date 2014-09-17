package test;

import static org.junit.Assert.*;

import java.io.*;
import java.util.List;

import org.junit.Test;

import settings.DBSettings;
import settings.ExperimentalSettings;
import settings.ExperimentalSettings.ComparisonType;
import settings.ExperimentalSettings.DifferenceOperators;
import views.View;
import api.SeeDB;

import com.google.common.collect.Lists;
import common.Utils;

import common.ConnectionPool;
import common.InputTablesMetadata;
import common.QueryExecutor;

public class AdityaTests {
	
	private String small 	= "table_10000000_20_5_1000_1000_1000_1000_1000_1000_1000_1000_100";
	private String large 	= "table_100000000_20_5_1000_1000_1000_1000_1000_1000_1000_1000_10";
	private String med	= "table_50000000_20_5_1000_1000_1000_1000_1000_1000_1000_1000_100";

	private String small1 	= "select * from "+ small + " where dim1_1000='2sqnug'";
	private String small2 	= "select * from "+ small + " where dim1_1000='x3zt3y'";
	private String small3 	= "select * from "+ small + " where dim1_1000='rnowj8'";
	private String med1 	= "select * from "+ med	  + " where dim1_1000 ='3llj45'";
	private String med2 	= "select * from "+ med   + " where dim1_1000='hkdd2g'";
	private String med3 	= "select * from "+ med   + " where dim1_1000='knb6u5'";
	private String large1 	= "select * from "+ large + " where dim1_1000='jc98tg'";
	private String large2 	= "select * from "+ large + " where dim1_1000='otyhcj'";
	private String large3 	= "select * from "+ large + " where dim1_1000='ccarhc'";
	
	private String[] small_queries 	= new String[]	{small1, 	small2, 	small3};
	private String[] medium_queries = new String[] 	{med1, 		med2, 		med3};
	private String[] large_queries 	= new String[] 	{large1, 	large2, 	large3};
	
	private String[][] queries 		= new String[][]{small_queries, medium_queries, large_queries};

	private String[] tables 		= new String[]	{small, med, large};

	//private int[] nconns 	= new int[]{2, 5, 10, 15, 20, 25, 30, 35, 40};
	private int[] nconns 	= new int[]{20, 30, 40};
	private int[] ndistinct = new int[]{2, 5, 10, 20};
	//private int[] ndistinct = new int[]{1, 3, 5, 10, 20, 25, 40, 50, 75, 100};

	private int[] agg_sizes = new int[]{5, 5, 5}; 

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

						for (int ii = 0; ii <1; ii++) {
							try {
								String s = null;
								Process p = Runtime.getRuntime().exec("echo 3 | sudo tee /proc/sys/vm/drop_caches");
								BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
								BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            							System.out.println("ok so far");
								while ((s = stdInput.readLine()) != null) {
                							System.out.println(s);		
            							}
            							while ((s = stdError.readLine()) != null) {
                							System.out.println(s);
           							}
							}
							catch (IOException e) {
         			   				System.out.println("exception happened - here's what I know: ");
            							e.printStackTrace();
            							System.exit(-1);
        						}
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
		
		settings.logFile 						= "testResults/" + settings.getDescriptor() + "_" + table +"_" +query;

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
