package microbenchmarks;

import java.sql.SQLException;

public class ConnectionPoolingTest {
	
	public static void main(String[] args) {
		//String[] tables = new String[] { "xs_1", "xs_2", "xs_3", "s_1", "s_2", "s_3" };
		String[] tables = new String[] {"table_10_2_2_3_2_1"};
		int[] nconns = new int[] {1, 10, 50, 90};
		for (int nconn : nconns) {
			for (String table : tables) {
				ConnectionPooling cp = new ConnectionPooling(nconn, "postgresql", 
						"127.0.0.1/seedb_data", "postgres", "postgrespwd");
				try {
					cp.runQueriesOnMultipleConnections(table); //"table_1m_10_10_test"); //"table_10_2_2_3_2_1");
				} catch (SQLException e) {
					e.printStackTrace();
					System.out.println("exception in sql execution");
				}
			}
		}
	}
}
