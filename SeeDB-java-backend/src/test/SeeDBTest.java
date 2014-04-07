package test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import output.OutputView;

import com.google.common.collect.Lists;

import common.DBSettings;
import common.ExperimentalSettings;
import common.ExperimentalSettings.ComparisonType;
import common.ExperimentalSettings.DifferenceOperators;
import common.InputTablesMetadata;
import common.QueryParser;
import common.Utils;
import difference_operators.CardinalityDifferenceOperator;
import difference_operators.DifferenceOperator;

import api.SeeDB;

public class SeeDBTest {
	private String defaultQuery1 = "select * from table_10_2_2_3_2_1 where measure1 < 2000";
	private String defaultQuery = "select * from table_10_2_2_3_2_1 where measure1 < 2000";
	private String defaultQuery2 = "select * from table_10_2_2_3_2_1 where measure1 >= 2000";
	
	@Test
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
	
	@Test
	public void connectToDatabaseTest() {
		SeeDB seedb = new SeeDB();
		seedb.connectToDatabase(1);
		assertTrue(seedb.getConnections()[1] != null);
	}
	
	@Test
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
	
	@Test
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
			List<OutputView> result = seedb.computeDifference();
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
			seedb.initialize(defaultQuery, defaultQuery, settings);
			List<OutputView> result = seedb.computeDifference();
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
			List<OutputView> result = seedb.computeDifference();
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
			List<OutputView> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	//@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryFullComparisonTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.mergeQueries = true;
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_FULL;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<OutputView> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
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
			List<OutputView> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	@Test
	public void endToEndAggregateGroupByDifferenceWithSingleQueryDifferenceComparisonTest() {
		SeeDB seedb = new SeeDB();
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.mergeQueries = true;
		settings.differenceOperators = Lists.newArrayList();
		settings.differenceOperators.add(DifferenceOperators.AGGREGATE);
		settings.comparisonType = ComparisonType.ONE_DATASET_DIFF;
		try {
			seedb.initialize(defaultQuery1, null, settings);
			List<OutputView> result = seedb.computeDifference();
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
			List<OutputView> result = seedb.computeDifference();
			Utils.printList(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

}
