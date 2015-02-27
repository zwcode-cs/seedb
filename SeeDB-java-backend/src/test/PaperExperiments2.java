package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.junit.Test;

import settings.DBSettings;
import settings.ExperimentalSettings;
import settings.ExperimentalSettings.Backend;
import settings.ExperimentalSettings.ComparisonType;
import settings.ExperimentalSettings.DifferenceOperators;
import views.AggregateGroupByView;
import views.View;

import api.SeeDB;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import common.Utils;

public class PaperExperiments2 {

	private boolean baseline = false;
	private enum GroupByOptimizations {NUMATTRS, HUFFMAN};
	private static String[] tables = {"s_1", "s_1_1", "s_1_2", "s_1_100k", "s_1_100k_1", 
		"s_1_100k_2", "s_1_500k", "s_1_500k_1", "s_1_500k_2", "s_1_3", "s_1_4", "s_1_5"};
	private static int[] rows = {1000000, 1000000,1000000, 100000, 100000, 100000,
		500000, 500000, 500000}; // missing a few
	private static String[] queries = {
		"select * from s_1 where dim29_50='6r2mr3'",
		"select * from s_1_1 where dim29_50='6r2mr3'",
		"select * from s_1_2 where dim29_50='6r2mr3'",
		"select * from s_1_100k where dim29_50='6r2mr3'",
		"select * from s_1_100k_1 where dim29_50='6r2mr3'",
		"select * from s_1_100k_2 where dim29_50='6r2mr3'",
		"select * from s_1_500k where dim29_50='6r2mr3'",
		"select * from s_1_500k_1 where dim29_50='6r2mr3'",
		"select * from s_1_500k_2 where dim29_50='6r2mr3'",
		"select * from s_1_3 where dim29_50='6r2mr3'",
		"select * from s_1_4 where selector=5",
		"select * from s_1_5 where selector=5"
	};
	private static Backend[] backends = {Backend.POSTGRES, Backend.VERTICA};
	private static int[] numAggAttrs = {1, 2, 5, 10, 15, 20};
	private static int[] numGBAttrs = {1, 2, 5, 10, 15, 20}; // {3, 4, 6, 7, 8, 9}; //
	private static int[] iters = {1, 20, 10, 3, 3, 0};
	private static int[] numConns = {16};//{40, 30, 20, 10, 5};
	private static String[] queriesWithDiffSelectivity = {};
	private static String[] workingMems = {};
	
	private void runSeeDB(String query, ExperimentalSettings exptSetting, DBSettings dbsetting) {
		long start 	= System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		seedb.connectToDatabase(dbsetting);
		try {
			seedb.initialize(query, null, exptSetting);
			List<View> result = seedb.computeDifference();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = null;
			if (exptSetting.logFile != null) {
				f = new File(exptSetting.logFile);
			}
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	private void runSeeDB2(String query, ExperimentalSettings exptSetting, DBSettings dbsetting) {
		long start 	= System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		seedb.connectToDatabase(dbsetting);
		try {
			seedb.initialize(query, null, exptSetting);
			List<View> result = seedb.computeDifferenceWrapper();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = null;
			if (exptSetting.logFile != null) {
				f = new File(exptSetting.logFile);
			}
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	/**
	 * get time required to execute things sequentially with no optimization
	 */
	public void getBaselines() {
		for (int i = 0; i < tables.length; i++) {
			for (Backend b : backends) {
				ExperimentalSettings settings 			= new ExperimentalSettings();
				settings.backend						= b;
				settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
				settings.differenceOperators 			= Lists.newArrayList();
				settings.noAggregateQueryOptimization 	= false;
				settings.optimizeAll					= false;
				settings.combineMultipleAggregates		= false;
				settings.combineMultipleGroupBys		= false;
				settings.useTempTables 					= false;
				settings.useParallelExecution 			= false;
				settings.mergeQueries 					= true;
				settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
				settings.logFile = "testResults/merged_" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
				if (b == Backend.POSTGRES) {
					runSeeDB(queries[i], settings, DBSettings.getPostgresDefault());
				} else if (b == Backend.VERTICA) {
					runSeeDB(queries[i], settings, DBSettings.getVerticaDefault());
				}	
			}
		}
	}
	
	public void getAggOptimizedSerial() {
		for (Backend b : backends) {
			for (int j = 0; j >= 0; j--) {
				for (int i = tables.length-1; i < tables.length; i++) {
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.combineMultipleAggregates		= true;
					settings.combineMultipleGroupBys		= false;
					settings.maxAggSize						= numAggAttrs[j];
					settings.useTempTables 					= false;
					settings.useParallelExecution 			= false;
					settings.mergeQueries 					= false;
					
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					settings.logFile = "testResults/" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
					if (b == Backend.POSTGRES) {
						runSeeDB(queries[i], settings, DBSettings.getPostgresDefault());
					} else if (b == Backend.VERTICA) {
						runSeeDB(queries[i], settings, DBSettings.getVerticaDefault());
					}
				}
			}
		}
	}
	
	public void getBaselineParallel() {
		for (Backend b : backends) {
			for (int j = 0; j < numConns.length; j++) {
				for (int i = 0; i < 1; i++) {
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.combineMultipleAggregates		= false;
					settings.combineMultipleGroupBys		= false;
					settings.maxDBConnections				= numConns[j];
					settings.useTempTables 					= false;
					settings.useParallelExecution 			= true;
					settings.mergeQueries 					= false;
					
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					settings.logFile = "testResults/" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
					if (b == Backend.POSTGRES) {
						runSeeDB(queries[i], settings, DBSettings.getPostgresDefault());
					} else if (b == Backend.VERTICA) {
						runSeeDB(queries[i], settings, DBSettings.getVerticaDefault());
					}
				}
			}
		}
	}
	
	public void getGBOptimizedSerial() {
		for (Backend b : backends) {
			for (int gb_idx = numGBAttrs.length - 1; gb_idx >= 0; gb_idx--) {
				for (int table_idx = 0; table_idx < 1; table_idx++) {
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.combineMultipleAggregates		= false;
					settings.combineMultipleGroupBys		= true;
					settings.useBinPacking					= true;
					settings.maxGroupBySize					= 100; //numGBAttrs[gb_idx];
					settings.useTempTables 					= false;
					settings.useParallelExecution 			= false;
					settings.mergeQueries 					= false;
					
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					for (int ii = 0; ii < 3; ii++) {
						settings.logFile = "testResults/bp_10000" + settings.getDescriptor() + "_" + tables[table_idx] +
								"_" +queries[table_idx];
						if (b == Backend.POSTGRES) {
							runSeeDB(queries[table_idx], settings, DBSettings.getPostgresDefault());
						} else if (b == Backend.VERTICA) {
							runSeeDB(queries[table_idx], settings, DBSettings.getVerticaDefault());
						}
					}
				}
				break;
			}
		}
	}
	
	public void getBestParallel() {
		for (Backend b : backends) {
			b= Backend.VERTICA;
			//for (int j = 0; j < numConns.length; j++) {
				for (int i = 0; i < 4; i++) { // tables.length
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.optimizeAll					= false;
					settings.combineMultipleAggregates		= true;
					settings.combineMultipleGroupBys		= true;
					settings.maxAggSize						= 5;
					settings.useBinPacking					= true;
					settings.maxGroupBySize					= 100;
					settings.maxDBConnections				= 16; //numConns[j];
					settings.useTempTables 					= true;
					settings.useParallelExecution 			= true;
					settings.mergeQueries 					= true;
					settings.num_rows						= rows[i];
					
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					settings.logFile = "testResults/new_test_" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
					if (b == Backend.POSTGRES) {
						runSeeDB(queries[i], settings, DBSettings.getPostgresDefault());
					} else if (b == Backend.VERTICA) {
						runSeeDB(queries[i], settings, DBSettings.getVerticaDefault());
					}
				}
			//}
			break;
		}
	}

	public void getParallelMABPruning() {
		for (Backend b : backends) {
			b= Backend.VERTICA;
			for (int i = 0; i < 4 ; i++) { //tables.length
				ExperimentalSettings settings 			= new ExperimentalSettings();
				settings.backend						= b;
				settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
				settings.differenceOperators 			= Lists.newArrayList();
				settings.optimizeAll					= false;
				settings.combineMultipleAggregates		= true;
				settings.combineMultipleGroupBys		= true;
				settings.maxAggSize						= 5;
				settings.useBinPacking					= true;
				settings.maxGroupBySize					= 100;
				settings.maxDBConnections				= 16;
				settings.useTempTables 					= true;
				settings.useParallelExecution 			= true;
				settings.mergeQueries 					= true;
				settings.MAB							= false;
				settings.num_rows						= rows[i];
				
				settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
				settings.logFile = "testResults/pruning_" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
				if (b == Backend.POSTGRES) {
					runSeeDB2(queries[i], settings, DBSettings.getPostgresDefault());
				} else if (b == Backend.VERTICA) {
					runSeeDB2(queries[i], settings, DBSettings.getVerticaDefault());
				}
			}
			break;
		}
	}
	
	public static void main(String[] args) {
		PaperExperiments2 pe = new PaperExperiments2();
		pe.getBestParallel();
		//pe.getParallelMABPruning();
	}

}
