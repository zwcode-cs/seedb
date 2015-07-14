package v2_test;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

import v2.DBSetting;
import v2.Attribute;
import v2.DBConnection;
import v2.DBMetadata;
import v2.DBMetadataCollector;

public class DBMetadataCollectorTest {

	@Test
	public void test() throws SQLException {
		DBConnection con = new DBConnection();
		assertTrue(con.connectToDatabase(DBSetting.getDefault()));
		DBMetadata metadata = DBMetadataCollector.collectMetadata(con, "xs_1");
		for (Attribute a : metadata.getAttributes()) {
			System.out.println(a);
		}
	}

}
