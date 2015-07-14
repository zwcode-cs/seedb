package v2;

import java.sql.ResultSet;
import java.sql.SQLException;

import v2.Attribute.AttributeType;

public class DBMetadataCollector {
	/**
	 * Get metadata for the table. 
	 * This does not take into account the input query, preferences or any 
	 * other info
	 * @param database
	 * @param table
	 * @throws SQLException 
	 */
	
	// there's a question about who manages access to the database
	public static DBMetadata collectMetadata(DBConnection connection, 
			String table) throws SQLException {
		DBMetadata metadata = new DBMetadata(table);
		// connect to the database and get details about the table
		ResultSet rs = connection.executeQuery("select * from " + table + "_schema");
		// TODO: add way to add number of distinct values
		while (rs.next()) {
			String attributeName = rs.getString(1);
			String attributeType = rs.getString(2);
			AttributeType type = AttributeType.OTHER;
			if (attributeType.equalsIgnoreCase("dimension")) {
				type = AttributeType.DIMENSION;
			} else if (attributeType.equalsIgnoreCase("measure")) {
				type = AttributeType.MEASURE;
			}
			
			Attribute a = new Attribute(attributeName, type, -1);
			metadata.addAttribute(a);
		}
		// for each attribute in table, add attribute metadata
		return metadata;
	}

}
