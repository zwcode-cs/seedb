package core;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

public class MetadataTest {

	@Test
	public void test() {
		QueryExecutor.Instantiate();
		Metadata metadata = new Metadata("election_data");
		try {
			metadata.updateTableSchema();
			// check number of dimension attributes, measure attributes, all attribtues and a few examples from each
			assertTrue(metadata.getDimensionAttributes().contains("contbr_st"));
			assertTrue(metadata.getMeasureAttributes().contains("contb_receipt_amt"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
