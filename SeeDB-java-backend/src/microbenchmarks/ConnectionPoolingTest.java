package microbenchmarks;

import java.sql.SQLException;

public class ConnectionPoolingTest {
	
	// vary the number of connections and tables and run the queries
	public static void main(String[] args) {
		String[] tables = new String[] { "s_2"};
		int[] nconns = new int[] {1, 10, 50, 90};
		
		for (int i = 0; i <1; i++) {
			for (int nconn : nconns) {
				for (String table : tables) {
					ConnectionPooling cp = new ConnectionPooling(nconn, "postgresql", 
							"host/db", "username", "password");
					try {
						cp.runQueriesOnMultipleConnections(table);
					} catch (SQLException e) {
						e.printStackTrace();
						System.out.println("exception in sql execution");
					}
				}
			}		
		}
	}
}
