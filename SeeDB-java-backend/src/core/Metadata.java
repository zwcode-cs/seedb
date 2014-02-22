package core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Metadata {
	private String table;
	private ArrayList<String> allAttributes;
	private ArrayList<String> dimensionAttributes;
	private ArrayList<String> measureAttributes;
	
	public Metadata(String table) {
		this.table = table;
	}
	
	public void updateTableSchema() throws SQLException {
		// get all the columns in the table, the dimension and the measure columns
		this.allAttributes = new ArrayList<String>();
		this.dimensionAttributes = new ArrayList<String>();
		this.measureAttributes = new ArrayList<String>();

		ResultSet rs = QueryExecutor.getTableColumns(table);
		
		while (rs.next()) {
			String attribute = rs.getString("COLUMN_NAME");
			String description = rs.getString("REMARKS");
			if ((description != null) && (description.toLowerCase().equals("measure")))
			{
				this.measureAttributes.add(attribute);
			}
			else if ((description != null) && (description.toLowerCase().equals("dimension"))) {
				this.dimensionAttributes.add(attribute);
			}
			this.allAttributes.add(attribute);
		}
	}
	
	public ArrayList<String> getDimensionAttributes() {
		return dimensionAttributes;
	}
	
	public ArrayList<String> getMeasureAttributes() {
		return measureAttributes;
	}

	public ArrayList<String> getAllAttributes() {
		return allAttributes;
	}
}
