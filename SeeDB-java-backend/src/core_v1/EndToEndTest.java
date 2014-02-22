package core_v1;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import core.DiscriminatingView;
import core.QueryExecutor;

public class EndToEndTest {

	@Test
	public void test() throws SQLException {
		// initialize InputQuery
		QueryExecutor.Instantiate();
		InputQuery input = new InputQuery("select * from election_data where contbr_receipt_amt <= 10");
		input.setFromClause("election_data");
		input.setWhereClause("contbr_st = 'MA'");
		
		// create instance of aggregate-based optimization
		AggregateBasedDifferenceOperator operator = new AggregateBasedDifferenceOperator(input, null, 2);
		
		
		operator.addOptimization(new GroupByOptimization(2, -1, 1, operator));
		operator.addOptimization(new MeasureAttributeOptimization(operator, 2));
		operator.addOptimization(new MergeQueriesOptimization(operator, false));
		
		List<DiscriminatingView> result = operator.computeDifference();
	}

}
