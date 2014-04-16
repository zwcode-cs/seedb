package common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.Lists;
import core.QueryExecutor;
import db_wrappers.DBConnection;

/**
 * Obtains metadata about attributes. Currently only determines if attribute
 * is measure or dimension. 
 * 
 * TODO: determine number of distinct values, type of attribute etc.
 * @author manasi
 *
 */
public class InputTablesMetadata {
	private List<Attribute> dimensionAttributes;
	private List<Attribute> measureAttributes;
	private List<String> tables;
	
	public List<Attribute> getDimensionAttributes() {
		return dimensionAttributes;
	}
	
	public List<Attribute> getMeasureAttributes() {
		return measureAttributes;
	}

	public InputTablesMetadata(List<String> tables, 
			DBConnection connection) {
		dimensionAttributes = Lists.newArrayList();
		measureAttributes = Lists.newArrayList();
		this.tables = tables;
		
		if (connection.getDatabaseType().equalsIgnoreCase("PostgreSQL")) {
			getAttributeMetadataByDescription(connection);
		}
		else {
			getAttributeMetadataByCount(connection);
		}
	}

	private void getAttributeMetadataByCount(DBConnection connection) {
		// TODO Auto-generated method stub
		
	}

	private void getAttributeMetadataByDescription(DBConnection connection) {
		for (String table : tables) {
			ResultSet rs = connection.getTableColumns(table);
			try {
				while (rs.next()) {
					String attribute = rs.getString("COLUMN_NAME");
					String description = rs.getString("REMARKS");
					if ((description != null) && (description.toLowerCase().equals("measure")))
					{
						this.measureAttributes.add(new Attribute(attribute));
					}
					else if ((description != null) && (description.toLowerCase().equals("dimension"))) {
						this.dimensionAttributes.add(new Attribute(attribute));
					}
				}
			} catch (SQLException e) {
				continue;
			}
		}
	}

	
	/**
	public boolean queriesSameTables(InputQuery q) {
		if (!q.database.equalsIgnoreCase(this.database)) return false;
		if (q.tables.size() != this.tables.size()) return false;
		Collections.sort(q.tables);
		Collections.sort(this.tables);
		for (int i = 0; i < this.tables.size(); i++) {
			if (!q.tables.get(i).equalsIgnoreCase(this.tables.get(i)))
				return false;
		}
		return true;
	}
	*/

}
