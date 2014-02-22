package core_v1;

import java.util.Map;
import com.google.common.collect.Maps;

public class ViewQueryResult {
	private ViewQueryBarebones viewQueryBarebones;
	private Map<String, ViewQueryResultRow> rows;
	
	public ViewQueryResult(ViewQueryBarebones viewQuery) {
		this.viewQueryBarebones = viewQuery;
		this.rows = Maps.newHashMap();
	}
	
	public ViewQueryBarebones getViewQueryBarebones() {
		return viewQueryBarebones;
	}

	public void addResultRow(ViewQueryResultRow row) {
		rows.put(row.getSerializedGroupByValues(), row);
	}
	
	public Map<String, ViewQueryResultRow> getRows() {
		return rows;
	}
	
}
