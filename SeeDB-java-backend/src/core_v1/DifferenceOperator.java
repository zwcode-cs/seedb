package core_v1;

import java.sql.SQLException;
import java.util.List;
import core.DiscriminatingView;
import core.Metadata;

public abstract class DifferenceOperator {
	protected AllTableMetadata allMetadata; // stores metadata about tables in query
	protected InputQuery inputQuery1; // may not be null
	protected InputQuery inputQuery2; // if null, indicates that we choose entire underlying table
	
	public DifferenceOperator(InputQuery inputQuery1, InputQuery inputQuery2) {
		initialize(inputQuery1, inputQuery2);
	}
	
	public DifferenceOperator(InputQuery inputQuery1) {
		this(inputQuery1, null);
	}
	
	/**
	 * Compute difference between results of the two queries
	 * @return 
	 * @throws SQLException 
	 */
	public abstract List<DiscriminatingView> computeDifference() 
		throws SQLException;

	public void initialize(InputQuery inputQuery1, InputQuery inputQuery2) {
		this.inputQuery1 = inputQuery1;
		this.inputQuery2 = inputQuery2;
		this.allMetadata = new AllTableMetadata();
		// assume that both queries query the same tables
		// TODO: allow queries to different tables
		List<String> tableNames = inputQuery1.getTableNames();
		for (String table : tableNames) {
			Metadata m = new Metadata(table);
			try {
				m.updateTableSchema();
				this.allMetadata.addMetadata(m);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

}
