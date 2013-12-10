package core;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import utils.DistributionUnit;

public class QueryProcessorTest {

	@Test
	public void ParseQueryTest() {
		String query = "select * from election_data where contbr_st = 'MA';";
		System.out.println(query);
		QueryProcessor queryProcessor = new QueryProcessor();
		queryProcessor.setQuery(query);
		queryProcessor.ParseQuery();
		assertEquals(queryProcessor.getTable(), "election_data");
		assertEquals(queryProcessor.getSelectPredicate(), "contbr_st");
	}
	
	@Test
	public void AddViewPredicatesTest() {
		String query = "select * from election_data where contbr_st = 'MA';";
		String aggQueryForQuery = "select contbr_st, sum(contb_receipt_amt) from election_data where contbr_st = 'MA' group by contbr_st;";
		String aggQueryForDataset = "select contbr_st, sum(contb_receipt_amt) from election_data group by contbr_st;";
		QueryProcessor queryProcessor = new QueryProcessor();
		queryProcessor.setQuery(query);
		queryProcessor.ParseQuery();
		assertEquals(queryProcessor.AddViewPredicates("contbr_st", Lists.newArrayList("contb_receipt_amt"), true).toLowerCase(), aggQueryForQuery.toLowerCase());
		assertEquals(queryProcessor.AddViewPredicates("contbr_st", Lists.newArrayList("contb_receipt_amt"), false).toLowerCase(), aggQueryForDataset.toLowerCase());
	}
	
	@Test
	public void GetDistributionForQuery() throws SQLException {
		QueryProcessor queryProcessor = new QueryProcessor();
		String query = "select contbr_st, sum(contb_receipt_amt) from election_data where contb_receipt_amt < 10 and contb_receipt_amt > 5 " +
				"group by contbr_st;";
		ArrayList<DistributionUnit> dist = new ArrayList<DistributionUnit>();
		dist.add(new DistributionUnit("AR", 0.09411764705));
		dist.add(new DistributionUnit("AZ", 0.60235294117));
		dist.add(new DistributionUnit("AL", 0.09882352941));
		dist.add(new DistributionUnit("CA", 0.13176470588));
		dist.add(new DistributionUnit("WA", 0.03764705882));
		dist.add(new DistributionUnit("MA", 0.03529411764));
		assertEquals(queryProcessor.GetDistributionForQuery(query), dist);
	}
	
	@Test
	// requires there to be only one dimension attribute for testing purposes, cand_nm
	public void ProcessTest() {
		QueryProcessor queryProcessor = new QueryProcessor();
		queryProcessor.setQuery("select * from election_data where contb_receipt_amt < 10 and contb_receipt_amt > 5;");
		List<DiscriminatingView> result = queryProcessor.Process();
		assertEquals(result.get(0).getUtility(), 0.05790716179, 1E-5);
		System.out.println(result.get(0).getCombinedDistribution());
	}

}
