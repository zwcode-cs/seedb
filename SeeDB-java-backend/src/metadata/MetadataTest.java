package metadata;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MetadataTest {
	public static void main(String[] args) {
		Metadata mg = new Metadata("table_1000_4_2_10_10_5_5_1", 1000, // table_1m_10_10_test, table_10_2_2_3_2_1
				"postgresql", "127.0.0.1/seedb_data", "postgres", "postgrespwd");
		try {
			
			List<String> columnNames = mg.getColumnNames();
			
			for (String columnName : columnNames) {
				System.out.println(columnName);
				
				Float variances = mg.getVariance(columnName);
				System.out.println(variances);
				
				Float numDistinct = mg.getNumDistinct(columnName);
				System.out.println(numDistinct);
				
				System.out.println("\n");
			}
			


		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
