package test;

import static org.junit.Assert.*;

import org.junit.Test;

import settings.DBSettings;


import db_wrappers.DBConnection;

public class DBConnectionTest {

	@Test
	public void isDBSupportedTest() {
		assertTrue(DBConnection.isDBSupported("postgreSQL"));
		assertTrue(DBConnection.isDBSupported("PostgreSQL"));
		assertFalse(DBConnection.isDBSupported("MySQL"));
	}
	
	@Test
	public void connectToDatabaseTest() {
		DBConnection con = new DBConnection();
		assertTrue(con.connectToDatabase(DBSettings.getDefault()));
	}
	
	@Test
	public void executeQueryTest() {
		fail("Not yet implemented");
	}
	
	@Test
	public void getTableColumnsTest() {
		fail("Not yet implemented");
	}

}
