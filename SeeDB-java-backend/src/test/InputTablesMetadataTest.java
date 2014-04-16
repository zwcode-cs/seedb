package test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import common.Attribute;
import common.DBSettings;
import common.InputQuery;
import common.InputTablesMetadata;
import common.Utils;

import db_wrappers.DBConnection;

public class InputTablesMetadataTest {

	@Test
	public void InputTablesMetadataConstructorTest() {
		List<Attribute> dimAttributes = Lists.newArrayList();
		dimAttributes.add(new Attribute("dim1"));
		dimAttributes.add(new Attribute("dim2"));
		List<Attribute> measureAttributes = Lists.newArrayList();
		measureAttributes.add(new Attribute("measure1"));
		measureAttributes.add(new Attribute("measure2"));
		
		DBConnection con = new DBConnection();
		con.connectToDatabase(DBSettings.getDefault());
		InputTablesMetadata metadata = new InputTablesMetadata(
				InputQuery.getDefault().tables, con);
		assertTrue(Utils.listEqual(metadata.getDimensionAttributes(), 
				dimAttributes));
		assertTrue(Utils.listEqual(metadata.getMeasureAttributes(), 
				measureAttributes));
	}
	
	@Test
	public void computeIntersectionTest() {
		DBConnection con = new DBConnection();
		con.connectToDatabase(DBSettings.getDefault());
		InputTablesMetadata metadata = new InputTablesMetadata(
				InputQuery.getDefault().tables, con);
		DBConnection con2 = new DBConnection();
		con2.connectToDatabase(DBSettings.getDefault());
		InputTablesMetadata metadata2 = new InputTablesMetadata(
				InputQuery.getDefault().tables, con2);
		//InputTablesMetadata.computeIntersection(metadata, metadata2);
	}

}
