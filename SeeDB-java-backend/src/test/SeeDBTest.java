package test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.junit.Test;

import views.View;

import com.google.common.collect.Lists;

import common.DBSettings;
import common.ExperimentalSettings;
import common.ExperimentalSettings.ComparisonType;
import common.ExperimentalSettings.DifferenceOperators;
import common.InputTablesMetadata;
import common.QueryParser;
import common.Utils;
import difference_operators.DifferenceOperator;
import api.SeeDB;

public class SeeDBTest {
	private String defaultQuery1 = "select * from election_data where cand_nm='McCain, John S'"; //"select * from table_10_2_2_3_2_1 where measure1 < 2000";
	private String defaultQuery = "select * from table_10_2_2_3_2_1 where measure1 < 2000";
	private String defaultQuery2 = "select * from table_10_2_2_3_2_1 where measure1 >= 2000";
	private String defaultQuery3 = "select * from election_data where cand_nm='McCain, John S'";
	private String defaultQuery4 = "select * from election_data where cand_nm='Cox, John H'";
	private String defaultQuery6 = "select * from s_1 where dim12_50='tlr2n4'";
	private String defaultQuery5 = "select * from xs_1_small where dim3_50='xcfhyb'";
	private String defaultQuery7 = "select * from m_1 where dim11_50='t7y9nt'";
	
	private String xs_1_query = "select * from xs_1 where dim3_50='7cdcrn'";
	//private String s_1_query = "select * from s_1 where dim5_50='8mo3f0'";
	private String s_1_query = "select * from s_1 where dim10_50='jm70ef'";
	private String s_1_same_query = "select * from s_1_same where dim1_100='35mtw7'";
	private String s_1_same_1000_query = "select * from s_1_same_1000 where dim1_1000='vn2dp6'";
	private String s_1_same_10_query = "select * from s_1_same_10 where dim1_10 ='b3ckzj'";
	//private String m_1_query = "select * from m_1 where dim4_50='5wvl86'";
	private String m_1_query = "select * from m_1 where dim15_50='adk7cz'";
	private String m_1_same_query = "select * from m_1_same where dim1_100='9ecz4e'";
	private String xs_2_query = "select * from xs_2 where dim3_50='8g9oat'";
	//private String s_2_query = "select * from s_2 where dim5_50='7h8329'";
	private String s_2_query = "select * from s_2 where dim4_50='rx2yq3'";
	private String s_2_same_query = "select * from s_2_same where dim1_100='448us2'";
	private String m_2_query = "select * from m_2 where dim6_50='9q8x7b'";
	private String xs_3_query = "select * from xs_3 where dim3_50='rc7jz5'";
	private String s_3_query = "select * from s_3 where dim7_50='c1s4u3'";
	private String m_3_query = "select * from m_3 where dim5_50='hcmfv4'";
	
	private String seeDBTestDir = "/Users/manasi/Public/seedb_test_results/";
	
	//private String[] istc_queries = new String[]{xs_1_query, xs_2_query, xs_3_query, 
	//											s_1_query, s_2_query, s_3_query, 
	//											m_1_query, m_2_query, m_3_query};
												
	//private String[] istc_queries = new String[]{s_1_query, s_2_query, m_1_query};
	//private String[] istc_queries = new String[]{s_1_same_query, s_2_same_query};
	private String[] istc_queries = new String[]{xs_1_query, s_1_same_query, m_1_same_query};
	private String[] local_queries = {defaultQuery5, defaultQuery6, defaultQuery7};
	private String[][] queries = new String[][]{istc_queries, local_queries};
	
	//private String[] istc_tables = new String[]{"xs_1", "xs_2", "xs_3", "s_1", "s_2", "s_3", "m_1", "m_2", "m_3"};
	private String[] local_tables = new String[]{"xs_1", "s_1", "m_1"};
	//private String[] istc_tables = new String[]{"s_1_same", "s_2_same"};
	private String[] istc_tables = new String[]{"xs_1", "s_1_same", "m_1_same"};
	//private String[] istc_tables = new String[]{"s_1", "s_2", "m_1"};
	private String[][] tables = new String[][]{istc_tables, local_tables};

	//private int[] istc_gb_sizes = new int[]{5, 5, 5, 50, 50, 50, 100, 100, 100};
	//private int[] istc_gb_sizes = new int[]{50, 50, 100};
	private int[] istc_gb_sizes = new int[]{5, 50, 100};
	private int[] local_gb_sizes = new int[]{5, 50, 100}; 
	private int[][] gb_sizes = new int[][]{istc_gb_sizes, local_gb_sizes};

	//private int[] istc_agg_sizes = new int[]{2, 2, 2, 5, 5, 5, 10, 10, 10};
	
	//private int[] istc_agg_sizes = new int[]{5, 5, 10};
	private int[] istc_agg_sizes = new int[]{2, 5, 10};
	private int[] local_agg_sizes = new int[]{2, 5, 10}; 
	private int[][] agg_sizes = new int[][]{istc_agg_sizes, local_agg_sizes};

	//private int[] ndistinct = new int[]{2};
	private int[] nconns = new int[]{5, 10, 20, 40};
	//private int[] ndistinct = new int[]{1, 2, 3, 5, 10, 20, 25, 40, 50, 75, 100};
	private int[] ndistinct = new int[]{1, 3, 5, 10, 20, 25, 40, 50, 75, 100};
	
	//@Test
	public void initializeTest() {
		SeeDB seedb = new SeeDB();
		try {
			String query1 = "select * from a where k";
			seedb.initialize(query1, null);
			assertEquals(seedb.getNumDatasets(), 1);
			assertEquals(seedb.getSettings().comparisonType, 
					ExperimentalSettings.ComparisonType.ONE_DATASET_FULL);
			assertTrue(seedb.getInputQueries()[0].equals(QueryParser.parse(query1,
					DBSettings.getDefault().database)));
			seedb.initialize("select * from a where k", 
					"select * from b where l");	
			assertEquals(seedb.getNumDatasets(), 2);
			assertEquals(seedb.getSettings().comparisonType, 
					ExperimentalSettings.ComparisonType.TWO_DATASETS);
			assertEquals(seedb.getNumDatasets(), 2);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void connectToDatabaseTest() {
		SeeDB seedb = new SeeDB();
		seedb.connectToDatabase();
		assertTrue(seedb.getConnection() != null);
	}
	
	//@Test
	public void getMetadataTest() {
		SeeDB seedb = new SeeDB();
		try {
			seedb.initialize(defaultQuery, defaultQuery);
			InputTablesMetadata[] metadatas = seedb.getMetadata();
			assertTrue((metadatas[0] != null) && (metadatas[1] != null));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void getDifferenceOperatorsTest() {
		SeeDB seedb = new SeeDB();
		try {
			seedb.initialize(defaultQuery, defaultQuery);
			List<DifferenceOperator> ops = seedb.getDifferenceOperators();
			assertEquals(ops.size(), 3); // number of difference operators supported
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndRowSampleTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.DATA_SAMPLE);
		settings.comparisonType = ComparisonType.TWO_DATASETS;
		try {
			seedb.initialize(defaultQuery, defaultQuery, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndCardinalityDifferenceTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.CARDINALITY);
		settings.comparisonType = ComparisonType.TWO_DATASETS;
		try {
			seedb.initialize(defaultQuery3, defaultQuery4, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.TWO_DATASETS;
		try {
			seedb.initialize(defaultQuery1, defaultQuery2, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithMergedQueriesTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.mergeQueries = true;
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.TWO_DATASETS;
		try {
			seedb.initialize(defaultQuery1, defaultQuery2, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryFullComparisonTest() {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.mergeQueries = false;//true; //false
		settings.noAggregateQueryOptimization = true;
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			System.out.println("Time taken: " + (System.currentTimeMillis() - start));
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryFullComparisonNoMergeTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryDifferenceComparisonTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.mergeQueries = true;
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.TWO_DATASETS;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryDifferenceComparisonNoMergeTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_DIFF;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndComputeManualTest() {
		SeeDB seedb = new SeeDB();
		try {
			seedb.initializeManual(defaultQuery);
			View v = seedb.computeManualView("dim1_3", "measure2", "count");
			v.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithTempTablesTest() {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_DIFF;
		//settings.useTempTables = true;
		//settings.mergeQueries = false;
		//settings.noAggregateQueryOptimization = true;
		settings.optimizeAll = true;
		//settings.useParallelExecution = true;
		try {
			seedb.initialize(defaultQuery5, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			System.out.println("Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	//@Test
	public void allTestsCopy() {
		int idx = 0;
		for (int i = 0; i < queries[idx].length; i++) {
			System.out.println(queries[idx][i]);
			//endToEndAggregateGroupByDifferenceSeqNoOpTest(queries[idx][i], tables[idx][i]);
			//endToEndAggregateGroupByDifferenceParallelNoOpTest(queries[idx][i], tables[idx][i]);
			//endToEndAggregateGroupByDifferenceSeqMergedQueriesTest(queries[idx][i], tables[idx][i]);
			//endToEndAggregateGroupByDifferenceParallelMergedQueriesTest(queries[idx][i], tables[idx][i]);
			
			/*
			for (int j = 0; j < ndistinct.length; j++){
				if (ndistinct[j] > agg_sizes[idx][i]) {
					break;
				}
				endToEndAggregateGroupByDifferenceSeqMultipleAggTest(queries[idx][i], tables[idx][i], ndistinct[j]);
				endToEndAggregateGroupByDifferenceParallelMultipleAggTest(queries[idx][i], tables[idx][i], ndistinct[j]);
			}
			*/
			
			
			for (int j = 1; j < ndistinct.length; j++){
				if (ndistinct[j] > gb_sizes[idx][i]) {
					break;
				}
				System.out.println("GBs:" + ndistinct[j]);
				//endToEndAggregateGroupByDifferenceParallelMultipleGBTest(queries[idx][i], tables[idx][i], ndistinct[j]);
				//endToEndAggregateGroupByDifferenceParallelAllOpTest(queries[idx][i], tables[idx][i], ndistinct[j], agg_sizes[idx][i]);
				//endToEndAggregateGroupByDifferenceSeqAllOpTest(queries[idx][i], tables[idx][i], ndistinct[j], agg_sizes[idx][i]);
				//endToEndAggregateGroupByDifferenceSeqMultipleGBTest(queries[idx][i], tables[idx][i], ndistinct[j]);
				
			}
		}
	}
	
	//@Test
	public void allTests() {
		int idx = 0;
		for (int k = 0; k < nconns.length; k++) {
			System.out.println("NCONNS:" + nconns[k]);
			for (int i = 2; i < 3; i++) { // queries.length
				System.out.println(queries[idx][i]);
				for (int j = 0; j < ndistinct.length; j++){
					if (ndistinct[j] > gb_sizes[idx][i]) {
						break;
					}
					System.out.println("NGB:" + ndistinct[j]);
					for (int ii = 0; ii <3; ii++) {
						endToEndAggregateGroupByDifferenceParallelAllOpTest(queries[idx][i], tables[idx][i], ndistinct[j], agg_sizes[idx][i], nconns[k]);//ndistinct[j], agg_sizes[idx][i], nconns[k]);
					}
				}
			}
		}
	}
	
	@Test
	public void allTestsNew() {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.useParallelExecution = false;
		settings.useTempTables = false;
		settings.maxDBConnections = 40;
		settings.combineMultipleAggregates = true;
		settings.combineMultipleGroupBys = true;
		settings.mergeQueries = false;
		try {
			seedb.initialize(defaultQuery5, null, settings);
			List<View> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	//@Test
	public void allTests2() {
		int idx = 0;
		for (int i = 6; i < queries[idx].length; i++) {
			System.out.println(queries[idx][i]);
			endToEndAggregateGroupByDifferenceSeqNoOpTest(queries[idx][i], tables[idx][i]);
			//endToEndAggregateGroupByDifferenceParallelNoOpTest(queries[idx][i], tables[idx][i]);
		}
	}
	
	//@Test
	public void dummyTest() {
		endToEndAggregateGroupByDifferenceSeqMultipleGBTest(xs_1_query, "xs_1", 2);
	}

	public void endToEndAggregateGroupByDifferenceSeqNoOpTest(String query, String table) {
		// no optimizations at all
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = true;
		settings.useParallelExecution = false;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	//@Test
	public void parallelQueriesTest() {
		String[] queries = {s_1_query, s_2_query, s_3_query};
		String[] tables = {"s_1", "s_2", "s_3"};
		
		for (int i = 0; i < queries.length; i++) {
			int[] conns = {90, 80, 60, 40};// 20};//, 10};
			for (int nconn : conns) {
				System.out.println(tables[i] + "__" + nconn);
				endToEndAggregateGroupByDifferenceParallelNoOpTestHelper(queries[i], tables[i], nconn);
			}
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelNoOpTest(String query, String table) {
		endToEndAggregateGroupByDifferenceParallelNoOpTestHelper(query, table, 20);
	}
		
	public void endToEndAggregateGroupByDifferenceParallelNoOpTestHelper(String query, String table, int nConns) {
		// no optimizations at all
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = true;
		settings.useParallelExecution = true;
		settings.maxDBConnections = nConns;
		settings.logFile = seeDBTestDir + "paralleltest_"  + nConns + "_" + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceSeqMergedQueriesTest(String query, String table) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = false;
		settings.combineMultipleGroupBys = false;
		settings.mergeQueries = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelMergedQueriesTest(String query, String table) {
		// no optimizations at all
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = false;
		settings.combineMultipleGroupBys = false;
		settings.mergeQueries = true;
		settings.useParallelExecution = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceSeqMultipleAggTest(String query, String table, int maxAggSize) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = true;
		settings.combineMultipleGroupBys = false;
		settings.mergeQueries = false;
		settings.useParallelExecution = false;
		settings.maxAggSize = maxAggSize;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelMultipleAggTest(String query, String table, int maxAggSize) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = true;
		settings.combineMultipleGroupBys = false;
		settings.mergeQueries = false;
		settings.useParallelExecution = true;
		settings.maxAggSize = maxAggSize;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}

	public void endToEndAggregateGroupByDifferenceSeqMultipleGBTest(String query, String table, int nGB) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = false;
		settings.combineMultipleGroupBys = true;
		settings.mergeQueries = true;
		settings.useParallelExecution = false;
		settings.maxGroupBySize = nGB;
		settings.useTempTables = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelMultipleGBTest(String query, String table, int nGB) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = false;
		settings.noAggregateQueryOptimization = false;
		settings.combineMultipleAggregates = false;
		settings.combineMultipleGroupBys = true;
		settings.mergeQueries = true;
		settings.useParallelExecution = true;
		settings.maxGroupBySize = nGB;
		settings.useTempTables = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceSeqAllOpTest(String query, String table, int nGB, int maxAggSize) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = true;
		settings.noAggregateQueryOptimization = false;
		settings.useParallelExecution = false;
		settings.maxGroupBySize = nGB;
		settings.maxAggSize = maxAggSize;
		settings.useTempTables = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelAllOpTest(String query, String table, int nGB, int maxAggSize, int nconns) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = true;
		settings.noAggregateQueryOptimization = false;
		settings.useParallelExecution = true;
		settings.maxGroupBySize = nGB;
		settings.maxAggSize = maxAggSize;
		settings.useTempTables = true;
		settings.maxDBConnections = nconns;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceSeqAllOpTempTableTest(String query, String table, int nGB) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = true;
		settings.noAggregateQueryOptimization = false;
		settings.useParallelExecution = false;
		settings.maxGroupBySize = nGB;
		settings.useTempTables = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
	
	public void endToEndAggregateGroupByDifferenceParallelAllOpTempTableTest(String query, String table, int nGB) {
		long start = System.currentTimeMillis();
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		settings.optimizeAll = true;
		settings.noAggregateQueryOptimization = false;
		settings.useParallelExecution = false;
		settings.maxGroupBySize = nGB;
		settings.useTempTables = true;
		settings.logFile = seeDBTestDir + settings.getDescriptor() + "_" + table;
		try {
			seedb.initialize(query, null, settings);
			List<View> result = seedb.computeDifference();
			//Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		} finally {
			File f = new File(settings.logFile);
			Utils.writeToFile(f, "Total time: " + (System.currentTimeMillis() - start));
		}
	}
}
