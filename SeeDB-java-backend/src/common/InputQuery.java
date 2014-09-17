package common;

import java.util.List;

/**
 * Represents a SQL input query received by SeeDB
 * We assume that the input query is of the form
 * SELECT * FROM A, B, C WHERE ...;
 * Currently projection, limit, groupby, subqueries are not supported
 * 
 * @author manasi
 */
public class InputQuery {
	public String rawQuery;
	public String fromClause;
	public String whereClause;
	public List<String> tables;
	
	public boolean equals(Object o) {
		if ((o == null) || (o.getClass() != this.getClass()))
			return false;
		else {
			InputQuery o_ = (InputQuery) o;
			return  rawQuery.equalsIgnoreCase(o_.rawQuery) &&
					fromClause.equalsIgnoreCase(o_.fromClause) &&
					whereClause.equalsIgnoreCase(o_.whereClause);
		}
	}
	
	public static InputQuery getDefault() {
		try {
			return QueryParser.parse("select * from table_10_2_2_3_2_1 where measure1 < 10");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static InputQuery deepCopy(InputQuery q) {
		InputQuery q1 = new InputQuery();
		q1.rawQuery = q.rawQuery;
		q1.fromClause = q.fromClause;
		q1.whereClause = q.whereClause;
		q1.tables = Utils.deepCopyList(q.tables);
		return q1;
	}
}
