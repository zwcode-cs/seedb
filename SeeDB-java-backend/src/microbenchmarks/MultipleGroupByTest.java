package microbenchmarks;

import java.sql.SQLException;

public class MultipleGroupByTest {
	public static void main(String[] args) {
		MultipleGroupBy mgb = new MultipleGroupBy("table_1m_10_10_test", 64, // table_1m_10_10_test, table_10_2_2_3_2_1
				"postgresql", "127.0.0.1/seedb_data", "postgres", "postgrespwd");
		try {
			mgb.runMultipleGroupByTest();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
