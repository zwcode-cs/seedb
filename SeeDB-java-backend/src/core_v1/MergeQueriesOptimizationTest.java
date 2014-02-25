package core_v1;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import utils.Constants;

import com.google.common.collect.Maps;

public class MergeQueriesOptimizationTest {
	InputQuery query;
	AggregateBasedDifferenceOperator op1;
	AggregateBasedDifferenceOperator op2;
	MergeQueriesOptimization mqopt1;
	MergeQueriesOptimization mqopt2;
	
	public MergeQueriesOptimizationTest() {
		Initialize();
	}
	
	public void Initialize() {
		query = new InputQuery("blah");
		op1 = new AggregateBasedDifferenceOperator(query, null, 2);
		op2 = new AggregateBasedDifferenceOperator(query, query, 2);
		mqopt1 = new MergeQueriesOptimization(op1, true);
		mqopt2 = new MergeQueriesOptimization(op1, false);
	}

	@Test
	public void rewriteQueriesTest() {
		fail("Not yet implemented");
	}
	
	@Test
	public void processResultsStreamingTest() {
		Map<String, ViewQueryResult> viewQueryResults = Maps.newHashMap();
		ViewQueryResultRow row = new ViewQueryResultRow();
		row.addAggregateValue("test1", 1.0);
		row.addGroupByValue("test2", "test2");
		row.setGroup(Constants.group1);
		ViewQueryResultRow newRow = mqopt2.processResultsStreaming(viewQueryResults, row);
		assertEquals(newRow, Constants.group2);
		assertEquals(newRow.toString().substring(0, newRow.toString().length()-1), 
				row.toString().substring(0, row.toString().length()-1));
	}

}
