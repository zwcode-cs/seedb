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
	private int numRows;
	
	public int getNumRows() {
		return 1;
	}
	
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
			//getAttributeMetadataByDescription(connection);
			getAttributeMetadataByName(connection);
		}
		else {
			getAttributeMetadataByCount(connection);
		}
		
		this.numRows = queryForNumRows(connection);
	}
	
	

	private int queryForNumRows(DBConnection connection) {
		ResultSet rs = connection.getNumRows(tables.get(0));
		try {
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
		return -1;
	}

	private void getAttributeMetadataByName(DBConnection connection) {
		for (String table : tables) {
			ResultSet rs = connection.getTableColumns(table);
			try {
				while (rs.next()) {
					String attribute = rs.getString("COLUMN_NAME");
					if (attribute.startsWith("measure"))
					{
						this.measureAttributes.add(new Attribute(attribute));
					}
					else if (attribute.startsWith("dim")) {
						this.dimensionAttributes.add(new Attribute(attribute));
					}
				}
			} catch (SQLException e) {
				continue;
			}
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
