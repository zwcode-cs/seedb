package core_v1;

import java.util.List;

import com.google.common.collect.Lists;


public class InputQuery {
	private String rawQuery;
	private String fromClause;
	private String whereClause;
	private String limitClause;
	private List<String> tableNames;
	
	public String getFromClause() {
		return this.fromClause;
	}
	
	public void setFromClause(String fromClause) {
		this.fromClause = fromClause;
		String[] tables = this.fromClause.split(",");
		for (String table : tables) {
			tableNames.add(table.trim());
		}
	}
	
	public String getWhereClause() {
		return this.whereClause;
	}
	
	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}
	
	public String getLimitClause() {
		return this.limitClause;
	}
	
	public void setLimitClause(String limitClause) {
		this.limitClause = limitClause;
	}
	
	public InputQuery(String rawQuery) {
		// TODO: parse query here
		this.rawQuery = rawQuery;
		this.tableNames = Lists.newArrayList();
	}
	
	public InputQuery(InputQuery query) {
		this.fromClause = query.fromClause;
		this.limitClause = query.limitClause;
		this.rawQuery = query.rawQuery;
		this.whereClause = query.whereClause;
		this.tableNames = Lists.newArrayList();
		String[] tables = this.fromClause.split(",");
		for (String table : tables) {
			tableNames.add(table.trim());
		}
	}
	
	public List<String> getTableNames() {
		return this.tableNames;
	}
}