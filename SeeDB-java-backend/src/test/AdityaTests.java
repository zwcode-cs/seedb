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
			seedb.initialize("" /* insert query1 here*/, null /* insert query 2 here*/, settings);
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
	}

}
