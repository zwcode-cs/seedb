package v2_test;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Test;

import v2.Attribute;
import v2.DBConnection;
import v2.DBMetadata;
import v2.DBMetadataCollector;
import v2.DBSetting;
import v2.InputQuery;
import v2.InvocationParameters;
import v2.Setting;
import v2.Setting.VizSource;
import v2.View;
import v2.ViewGenerator;

public class ViewGeneratorTest {

	@Test
	public void test() throws SQLException {
		DBConnection con = new DBConnection();
		InvocationParameters params = new InvocationParameters();
		params.comparativeVisualization = true;
		params.vizSource = VizSource.SEEDB;
		
		assertTrue(con.connectToDatabase(DBSetting.getDefault()));
		DBMetadata metadata = DBMetadataCollector.collectMetadata(con, "xs_1");
		ArrayList<View> views = ViewGenerator.generateViewStubs(metadata, null, 
				null, params, null);
		for (View v : views) {
			System.out.println(v);
		}
	}

}
