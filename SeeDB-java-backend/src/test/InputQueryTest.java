package test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.collect.Lists;

import common.InputQuery;

public class InputQueryTest {

	@Test
	public void queriesSameTablesTest() {
		InputQuery in1 = new InputQuery();
		InputQuery in2 = new InputQuery();
		in1.database = "db";
		in2.database = "db";
		in1.tables = Lists.newArrayList();
		in2.tables = Lists.newArrayList();
		in1.tables.add("test2");
		in1.tables.add("test1");
		in2.tables.add("test1");
		assertFalse(in1.queriesSameTables(in2));
		in2.tables.add("test2");
		assertTrue(in2.queriesSameTables(in1));
		in1.database = "db'";
		assertFalse(in2.queriesSameTables(in1));
	}

}
