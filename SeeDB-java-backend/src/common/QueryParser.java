package common;

import java.util.Collections;

import com.google.common.collect.Lists;

import db_wrappers.DBConnection;

/**
 * Parses raw SQL query provided to SeeDB
 * Currently a simple string matching algorithm
 * TODO: use JsqlParser
 * @author manasi
 *
 */
public class QueryParser {

	public static InputQuery parse(String query) throws Exception {
		// string semicolon from the end of the query
		if (query.endsWith(";")) {
			query = query.substring(0, query.length() - 1);
		}
		
		InputQuery in = new InputQuery();
		in.rawQuery = query.trim();
		String lowerCase = query.toLowerCase();
		int fromLength = 4;
		int whereLength = 5;
		int fromIdx = lowerCase.indexOf("from");
		if (fromIdx < 0) throw new Exception("no from keyword");
		int whereIdx = lowerCase.indexOf("where");
		if (whereIdx < 0) throw new Exception("no where keyword");
		in.fromClause = query.substring(fromIdx + fromLength, whereIdx).trim();
		in.whereClause = query.substring(whereIdx + whereLength).trim();
		String[] tables = in.fromClause.split(",");
		in.tables = Lists.newArrayList();
		for (String table : tables) {
			in.tables.add(table.trim());
		}
		Collections.sort(in.tables);
		return in;
	}
}
