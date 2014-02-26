package microbenchmarks;

import java.sql.SQLException;

public class MultipleGroupByTest {
	public static void main(String[] args) {
		//String[] tables = new String[] { "xs_1", "xs_2", "xs_3", "s_1", "s_2", "s_3" };
		String[] tables = new String[] {"table_10_2_2_3_2_1"};
		for (int mem = 1000; mem < 10000000; mem=mem*10) {
			for (String table : tables) {
				MultipleGroupBy mgb = new MultipleGroupBy(table, mem, // table_1m_10_10_test, table_10_2_2_3_2_1
						"postgresql", "127.0.0.1/seedb_data", "postgres", "postgrespwd");
				try {
					mgb.runMultipleGroupByTest();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
