package metadata;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MetadataTest {
	public static void main(String[] args) {
		Metadata mg = new Metadata("table_1000_4_2_10_10_5_5_1", 1000, // table_1m_10_10_test, table_10_2_2_3_2_1
				"postgresql", "127.0.0.1/seedb_data", "postgres", "postgrespwd");
		try {
			
			Map<String, String> columnAttributes = mg.getColumnAttributes();
			Integer numRows = mg.getNumRows();
			System.out.println(numRows);
			
			for (Entry<String, String> columnAttribute : columnAttributes.entrySet()) {
				
				String columnName = columnAttribute.getKey();
				String columnType = columnAttribute.getValue();
				
				System.out.println(columnName);
				
				Float variance = mg.getVariance(columnName);
				System.out.println(variance);
				
				Integer numDistinct = mg.getNumDistinct(columnName);
				System.out.println(numDistinct);
				
				String type = mg.getType(columnName, columnType, numRows, numDistinct);
				System.out.println(type);
				
				System.out.println("\n");
			}
			


		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
