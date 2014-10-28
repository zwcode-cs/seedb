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

public class PaperExperiments {

	private boolean baseline = false;
	private enum GroupByOptimizations {NUMATTRS, HUFFMAN};
	private static String[] tables = {"xs_2", "s_1", "m_1", "xs_3", "s_2", "m_2"};
	private static String[] queries = {
		"select * from xs_2 where dim3_50='96jq04'", // checked
		"select * from s_1 where dim10_50='04fcm6'", // checked
		"select * from m_1 where dim15_50='nrl5qn'", // checked
		"select * from xs_3 where dim3_50='99tq6h'", // checked
		"select * from s_2 where dim4_50='c1860w'",  // checked
		"select * from m_2 where dim20_50='7zryg4'"}; // checked
	private static Backend[] backends = {Backend.POSTGRES, Backend.VERTICA};
	private static int[] numAggAttrs = {2, 5, 10};
	private static int[] numGBAttrs = {2, 5, 10, 20, 50, 75, 100};
	private static int[] maxAggs = {2, 5, 10, 2, 5, 10};
	private static int[] maxGBs = {5, 10, 100, 5, 10, 100};
	private static int[] numConns = {40, 30, 20, 10, 5, 1};
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
	/**
	 * get time required to execute things sequentially with no optimization
	 */
	public void getBaselines() {
		for (int i = tables.length - 1; i < tables.length; i++) {
			for (Backend b : backends) {
				ExperimentalSettings settings 			= new ExperimentalSettings();
				settings.backend						= b;
				settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
				settings.differenceOperators 			= Lists.newArrayList();
				settings.noAggregateQueryOptimization 	= true;
				settings.useTempTables 					= false;
				settings.useParallelExecution 			= false;
				settings.mergeQueries 					= false;
				settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
				settings.logFile = "/Users/Manasi/Public/testResults/" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
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
			for (int j = numAggAttrs.length - 1; j >= 0; j--) {
				for (int i = 0; i < tables.length; i++) {
					if (numAggAttrs[j] > maxAggs[i]) {
						continue;
					}
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.combineMultipleAggregates		= true;
					settings.combineMultipleGroupBys		= false;
					settings.maxAggSize						= 10; //numAggAttrs[j];
					settings.useTempTables 					= false;
					settings.useParallelExecution 			= false;
					settings.mergeQueries 					= false;
					if (i == 0) {
						settings.makeGraphs = true;
					}
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					settings.logFile = "testResults/" + settings.getDescriptor() + "_" + tables[i] +"_" +queries[i];
					for (int ii = 0; ii < 3; ii++) {
						if (b == Backend.POSTGRES) {
							runSeeDB(queries[i], settings, DBSettings.getPostgresDefault());
						} else if (b == Backend.VERTICA) {
							runSeeDB(queries[i], settings, DBSettings.getVerticaDefault());
						}
					}
					break;
				}
			}
			break;
		}
	}
	
	public void getGBOptimizedSerial() {
		for (Backend b : backends) {
			b = Backend.VERTICA;
			for (int gb_idx = numGBAttrs.length - 1; gb_idx >= 2; gb_idx--) {
				for (int table_idx = 0; table_idx < tables.length; table_idx++) {
					table_idx = 1;
					if (numGBAttrs[gb_idx] > maxGBs[table_idx]) {
						continue;
					}
					ExperimentalSettings settings 			= new ExperimentalSettings();
					settings.backend						= b;
					settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
					settings.differenceOperators 			= Lists.newArrayList();
					settings.combineMultipleAggregates		= false;
					settings.combineMultipleGroupBys		= true;
					settings.maxGroupBySize					= numGBAttrs[gb_idx];
					settings.useTempTables 					= false;
					settings.useParallelExecution 			= false;
					settings.mergeQueries 					= false;
					
					settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
					settings.logFile = "testResults/" + settings.getDescriptor() + "_" + tables[table_idx] +
							"_" +queries[table_idx];
					for (int ii = 0; ii < 3; ii++) {
						if (b == Backend.POSTGRES) {
							runSeeDB(queries[table_idx], settings, DBSettings.getPostgresDefault());
						} else if (b == Backend.VERTICA) {
							runSeeDB(queries[table_idx], settings, DBSettings.getVerticaDefault());
						}
					}
				}
			}
			break;
		}
	}
	
	//@Test
	public void getUserStudyData() {
		String query = "select * from diabetes_data where dim_age='[30-40)'";
		ExperimentalSettings settings 			= new ExperimentalSettings();
		settings.comparisonType 				= ComparisonType.ONE_DATASET_FULL;
		settings.differenceOperators 			= Lists.newArrayList();
		settings.combineMultipleAggregates		= true;
		settings.combineMultipleGroupBys		= false;
		settings.maxAggSize						= 2;
		settings.useTempTables 					= false;
		settings.useParallelExecution 			= false;
		settings.mergeQueries 					= false;
		settings.makeGraphs = true;
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		runSeeDB(query, settings, DBSettings.getLocalDefault());	
	}
	
	public static void main(String[] args) {
		PaperExperiments pe = new PaperExperiments();
		pe.getGBOptimizedSerial();
	}

}
