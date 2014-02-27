package microbenchmarks;

import java.sql.SQLException;

public class MultipleAttributeTest {
	public static void main(String[] args) {
		MultipleAttribute ma = new MultipleAttribute("table_1000_4_2_10_10_5_5_1", 1000, // table_1m_10_10_test, table_10_2_2_3_2_1
				"postgresql", "127.0.0.1/seedb_data", "postgres", "postgrespwd");
		try {
			ma.runMultipleAttributeTest();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
