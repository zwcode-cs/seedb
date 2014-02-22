package common;

import java.sql.ResultSet;
import java.sql.SQLException;
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
	private InputQuery inputQuery;
	
	public List<Attribute> getDimensionAttributes() {
		return dimensionAttributes;
	}
	
	public List<Attribute> getMeasureAttributes() {
		return measureAttributes;
	}

	public InputTablesMetadata(InputQuery inputQuery, 
			DBConnection connection) {
		dimensionAttributes = Lists.newArrayList();
		measureAttributes = Lists.newArrayList();
		this.inputQuery = inputQuery;
		
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
		for (String table : inputQuery.tables) {
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

	// TODO: flesh this out for not querying the same tables
	public static void computeIntersection(
			InputTablesMetadata inputTablesMetadata,
			InputTablesMetadata inputTablesMetadata2) {
		if (inputTablesMetadata.inputQuery.queriesSameTables(
				inputTablesMetadata2.inputQuery))
			return;
	}

}
