package microbenchmarks;

import java.sql.SQLException;

public class MultipleAggregateTest {
	public static void main(String[] args) {
		int[] working_mem = new int[]{1000, 10000, 100000, 1000000};
		String[] tables = new String[]{"m_1", "m_2", "m_3"};
		String[] selectedDimAttribute = new String[] {"dim9_5000", "dim49_5000", "dim55_5000"};
		int nMeasures = 10;
		for (int mem : working_mem) {
			for (int i = 0; i < tables.length; i++) {
				MultipleAggregate mg = new MultipleAggregate(tables[i], mem, selectedDimAttribute[i], nMeasures,
						"postgresql", "host/db", "username", "password");
				try {
					mg.runMultipleAggregateTest();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
