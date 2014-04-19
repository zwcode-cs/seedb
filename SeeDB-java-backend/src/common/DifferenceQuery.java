package common;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class DifferenceQuery {
	InputQuery[] inputQueries;
	public ExperimentalSettings.DifferenceOperators op;
	public List<DifferenceQuery> derivedFrom;
	public boolean mergedQueries;
	
	public List<Attribute> selectAttributes;
	public List<Attribute> aggregateAttributes;
	public List<Attribute> groupByAttributes;	
	public String limitClause;
	public String additionalWherePredicates;
	public List<List<String>> aggregateFunctions;
	
	public List<String> getSQLQuery() {
		List<String> ret = Lists.newArrayList();
		if (mergedQueries) {
			ret.add(getSQLQueryHelper(true, inputQueries[0], inputQueries[1]));
		}
		else {
			ret.add(getSQLQueryHelper(false, inputQueries[0], null));
			ret.add(getSQLQueryHelper(false, inputQueries[1], null));
		}
		return ret;
	}
	
	public String getSQLQueryHelper(boolean combined, InputQuery q1, InputQuery q2) {
		String result = "";
		result += "SELECT";
		boolean needsComma = false;
		
		if (!selectAttributes.isEmpty()) {
			result += " " + Joiner.on(", ").join(
					getAttributeNames(selectAttributes));
		}
		
		if (needsComma) {
			result += ", ";
		} else {
			result += " ";
		}
		needsComma = false;
		
		if (!groupByAttributes.isEmpty()) {	
			result += " " + Joiner.on(", ").join(
					getAttributeNames(groupByAttributes));
			needsComma = true;
		}
		
		if (needsComma) {
			result += ", ";
		} else {
			result += " ";
		}
		needsComma = false;
		
		if (combined) {
			result += "case when " + q1.whereClause + " then 1 else 0 end as seedb_group_1";
			result += " ,";
			if (q2.whereClause == null || q2.whereClause.isEmpty()) {
				result += "1 as seedb_group_2";
			}
			else {
				result += "case when " + q2.whereClause + " then 1 else 0 end as seedb_group_2";
			}
			needsComma = true;
		}
		
		if (needsComma) {
			result += ", ";
		} else {
			result += " ";
		}
		needsComma = false;
		
		if (!aggregateAttributes.isEmpty()) {
			List<String> aggregates = 
					applyAggregateFunctionsToAttributes(aggregateAttributes, aggregateFunctions);
			result += Joiner.on(", ").join(aggregates);
		}
		
		if (!combined) {
			result += " FROM " + q1.fromClause;
			result += ((q1.whereClause != null && !q1.whereClause.isEmpty()) ? " WHERE " + 
						q1.whereClause : "");
		}
		else {
			result += " FROM " + q1.fromClause;
			// TODO: do I need a where extension?
		}
		
		if (!groupByAttributes.isEmpty()) {
			result += " GROUP BY " + Joiner.on(", ").join(
				getAttributeNames(groupByAttributes));
		}
		
		if (combined) {
			result += " , seedb_group_1, seedb_group_2";
		}
		return result;
	}
	
	public static List<String> applyAggregateFunctionsToAttributes(
			List<Attribute> attrs, List<List<String>> aggregateFunctions) {
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < attrs.size(); i++) {
			for (int j = 0; j < aggregateFunctions.get(i).size(); j++) {
				list.add(aggregateFunctions.get(i).get(j) + "(" + attrs.get(i).name + ") as " + 
						(attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j));
			}
		}
		return list;
	}
	
	public static List<String> getColumnNamesForAggregateAttributes(
			List<Attribute> attrs, List<List<String>> aggregateFunctions) {
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < attrs.size(); i++) {
			for (int j = 0; j < aggregateFunctions.get(i).size(); j++) {
				list.add((attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j));
			}
		}
		return list;
	}

	public static List<String> getAttributeNames(List<Attribute> attrs) {
		List<String> list = Lists.newArrayList();
		for (Attribute attr : attrs) {
			list.add(attr.name);
		}
		return list;
	}

	public void addAggregateAttribute(Attribute attribute,
			List<String> aggFuncs) {
		aggregateAttributes.add(attribute);
		aggregateFunctions.add(aggFuncs);
	}

	
	public DifferenceQuery(ExperimentalSettings.DifferenceOperators op, InputQuery[] inputQueries) {
		this.op = op;
		this.inputQueries = inputQueries;
		selectAttributes = Lists.newArrayList();
		aggregateAttributes = Lists.newArrayList();
		groupByAttributes = Lists.newArrayList();
		derivedFrom = Lists.newArrayList();
		aggregateFunctions = Lists.newArrayList();
	}
	
	public String getSerializedGroupByAttributes() {
		List<String> attrs = getAttributeNames(groupByAttributes);
		Collections.sort(attrs);
		return Joiner.on("__").join(attrs);
	}

	public static DifferenceQuery deepCopy(DifferenceQuery query) {
		DifferenceQuery dq = new DifferenceQuery(query.op, query.inputQueries);
		dq.mergedQueries = query.mergedQueries;
		dq.limitClause = query.limitClause;
		dq.additionalWherePredicates = query.additionalWherePredicates;
		dq.selectAttributes = Utils.deepCopyList(query.selectAttributes);
		dq.aggregateAttributes = Utils.deepCopyList(query.aggregateAttributes);
		dq.groupByAttributes = Utils.deepCopyList(query.groupByAttributes);
		dq.aggregateFunctions = Utils.deepCopyListOfLists(query.aggregateFunctions);
		return dq;
	}
	
	public String getSerializedGroupByAndAggregateAttributes() {
		List<String> attrs = getAttributeNames(aggregateAttributes);
		Collections.sort(attrs);
		return getSerializedGroupByAttributes() + "____" + Joiner.on("__").join(attrs);
	}
		
}
