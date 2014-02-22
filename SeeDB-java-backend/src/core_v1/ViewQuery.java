package core_v1;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ViewQuery {
	private ViewQueryBarebones viewQueryBarebones;
	private List<String> additionalWherePredicates;
	private List<String> orderByAttributes;
	private InputQuery input;
	private boolean twoQueriesAsOne;
	
	public ViewQuery(ViewQueryBarebones viewQueryBarebones) {
		additionalWherePredicates = new ArrayList<String>();
		this.viewQueryBarebones = viewQueryBarebones;
		orderByAttributes = new ArrayList<String>();
	}
	
	public ViewQuery clone() {
		ViewQuery res = new ViewQuery(this.getViewQueryBarebones());
		res.additionalWherePredicates.addAll(this.additionalWherePredicates);
		res.orderByAttributes.addAll(this.orderByAttributes);
		res.input = this.input;
		res.setTwoQueriesAsOne(this.getTwoQueriesAsOne());
		return res;
	}
	
	public ViewQueryBarebones getViewQueryBarebones() {
		return this.viewQueryBarebones;
	}

	public String convertToSQLQuery() {
		String result = "";
		result += "SELECT";
		result += " " + Joiner.on(", ").join(
				this.viewQueryBarebones.getGroupByAttributes());
		List<String> aggregatesWithSum = Lists.transform(
				this.viewQueryBarebones.getAggregateAttributes(), 
				new Function<String, String>() {
		             public String apply(String measureAttribute) {
		                     return "SUM(" + measureAttribute + ")";
		             }
				});
		result += "," + Joiner.on(", ").join(aggregatesWithSum);
		result += " FROM " + this.input.getFromClause();
		result += ((this.input.getWhereClause() != null) ? " WHERE " + 
					this.input.getWhereClause() : "");
		result += " GROUP BY " + Joiner.on(", ").join(
				this.viewQueryBarebones.getGroupByAttributes());
		// TODO: do we need an order by
		return result;
	}

	public boolean getTwoQueriesAsOne() {
		return twoQueriesAsOne;
	}

	public void setTwoQueriesAsOne(boolean twoQueriesAsOne) {
		this.twoQueriesAsOne = twoQueriesAsOne;
	}

	public void setInput(InputQuery inputQuery) {
		this.input = inputQuery;
		
	}

	public InputQuery getInput() {
		return input;
	}

}
