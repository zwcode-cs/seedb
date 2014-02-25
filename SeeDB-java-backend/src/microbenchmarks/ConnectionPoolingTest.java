package microbenchmarks;

import java.sql.SQLException;

public class ConnectionPoolingTest {
	
	public static void main(String[] args) {
		ConnectionPooling cp = new ConnectionPooling(10, "postgresql", 
				"127.0.0.1/seedb_data", "postgres", "postgrespwd");
		try {
			cp.runQueriesOnMultipleConnections("table_10_2_2_3_2_1"); //"table_1m_10_10_test"); //"table_10_2_2_3_2_1");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("exception in sql execution");
		}
	}
}
