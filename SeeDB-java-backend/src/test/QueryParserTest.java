package test;

import static org.junit.Assert.*;

import org.junit.Test;

import common.InputQuery;
import common.QueryParser;

public class QueryParserTest {

	@Test
	public void parseTest() {
		String query = "select * from a, b where a.id=b.id and b.value < 10";
		InputQuery q;
		try {
			q = QueryParser.parse(query);
			assertEquals(q.fromClause, "a, b");
			assertEquals(q.whereClause, "a.id=b.id and b.value < 10");
			assertEquals(q.tables.size(), 2);
			assertEquals(q.tables.get(0), "a");
			assertEquals(q.tables.get(1), "b");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}

}
