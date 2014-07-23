package microbenchmarks;

import java.sql.SQLException;
 
public class TwoQueriesAsOneTest {

	public static void main(String[] args) {
		int working_mem = 1000000; //1GB
		String[] tables = new String[] {"xs_1", "s_1", "m_1"};
		String[] selectedDimAttr = new String[] {"dim4_200", "dim12_200", "dim14_200"};
		String selectAttribute = "measure1";
		String aggregateAttribute = "measure2";
		try {
			for (int i = 0; i < tables.length; i++) {
				TwoQueriesAsOne tmp = new TwoQueriesAsOne(tables[i], working_mem, selectedDimAttr[i], selectAttribute,
						aggregateAttribute, "postgresql", "dbname", "username", "pwd");
				tmp.runTest();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
