package common;

import java.util.Collections;
import java.util.List;

import settings.ExperimentalSettings;
import utils.Constants;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
/**
 * Represents the queries underlying a view.
 * It can be thought of as the input query + some predicates
 * As a result, there are two input queries and a set of additional
 * predicates that are make up the view
 * @author manasi
 *
 */
public class DifferenceQuery implements Comparable<DifferenceQuery>{
	private static int seqNumCounter = 0;				// used for naming of temp tables
	int seqNum;											// same
	
	InputQuery[] inputQueries;							// input queries for this view
	public ExperimentalSettings.DifferenceOperators op;	// what kind of operator created this difference query
	public List<DifferenceQuery> derivedFrom;			// the optimizer combines multiple queries into one, so where did this query come from?
	public boolean mergedQueries;						// while writing SQL, should the target and comparison query be merged?
	
	// view-specific attributes
	public List<Attribute> selectAttributes;			// additional select attributes
	public List<Attribute> aggregateAttributes;			// additional aggregate attributes
	public List<List<String>> aggregateFunctions;		// aggregate functions corresponding to the aggregate functions
	public List<Attribute> groupByAttributes;			// additional group by attributes
	public String limitClause;							// additional limit clause
	public String additionalWherePredicates;			// additional where clause
	
	/**
	 * constructor
	 */
	public DifferenceQuery() {
		incrementSeqNum();
		selectAttributes = Lists.newArrayList();
		aggregateAttributes = Lists.newArrayList();
		groupByAttributes = Lists.newArrayList();
		derivedFrom = Lists.newArrayList();
		aggregateFunctions = Lists.newArrayList();
	}
	
	/**
	 * constructor
	 * @param op
	 * @param inputQueries
	 */
	public DifferenceQuery(ExperimentalSettings.DifferenceOperators op, InputQuery[] inputQueries) {
		this();
		this.seqNum = seqNumCounter++;
		this.op = op;
		this.inputQueries = inputQueries;
	}
	
	private synchronized void incrementSeqNum() {
		this.seqNum = seqNumCounter++;
	}
	
	/**
	 * get the SQL queries corresponding to the view
	 * @return
	 */
	public List<String> getSQLQuery() {
		return getSQLQuery(false /*use temp table*/);
	}
	
	/**
	 * use sequence number to get the temp table name for this view
	 * @return
	 */
	public String getTempTableName() {
		return Constants.tempTablePrefix + seqNum;
	}
	
	/**
	 * get maximum number of distinct values for the given set of group by
	 * attributes
	 * @return
	 */
	public int getMaxDistinct() {
		int i = 1;
		for (Attribute a : groupByAttributes) {
			i *= a.numDistinctValues;
		}
		return i;
	}
	
	/**
	 * used to sort by # of maximum distinct values
	 */
	public int compareTo(DifferenceQuery dq) {
		return this.getMaxDistinct() - dq.getMaxDistinct();
	}
	
	/**
	 * Get SQL queries for all the views from which this view is derived
	 * @param fromTempTable
	 * @return
	 */
	public List<String> getSQLForParentQueries(boolean fromTempTable) {
		List<String> ret = Lists.newArrayList();
		for (DifferenceQuery dq : this.derivedFrom) {
			// no merging allowed for now
			ret.add(dq.getSQLQueryHelper(false, dq.inputQueries[0], null, false, fromTempTable ? "seedb_tmp_table_"+seqNum : null, 1));
			ret.add(dq.getSQLQueryHelper(false, dq.inputQueries[1], null, false, fromTempTable ? "seedb_tmp_table_"+seqNum : null, 2));
		}
		return ret;
	}
	
	/**
	 * create SQL queries for view; optionally modify query to put results into a temp table
	 * @param insertIntoTempTable
	 * @return
	 */
	public List<String> getSQLQuery(boolean insertIntoTempTable) {
		List<String> ret = Lists.newArrayList();
		if (mergedQueries) {
			ret.add(getSQLQueryHelper(true, inputQueries[0], inputQueries[1], insertIntoTempTable, null, -1));
		}
		else {
			ret.add(getSQLQueryHelper(false, inputQueries[0], null, insertIntoTempTable, null, -1));
			ret.add(getSQLQueryHelper(false, inputQueries[1], null, insertIntoTempTable, null, -1));
		}
		return ret;
	}
	
	/**
	 * Core helper function that actually builds the SQL query
	 * @param combined
	 * @param q1
	 * @param q2
	 * @param insertIntoTempTable
	 * @param fromTempTable
	 * @param group
	 * @return
	 */
	public String getSQLQueryHelper(boolean combined, InputQuery q1, InputQuery q2, boolean insertIntoTempTable, String fromTempTable, int group) {
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
			List<String> aggregates = null;
			if (fromTempTable != null) {
				aggregates = getAggregateAttributeColumnNames(aggregateAttributes, aggregateFunctions);
			}
			else {
				aggregates = applyAggregateFunctionsToAttributes(aggregateAttributes, aggregateFunctions);
			}
			result += Joiner.on(", ").join(aggregates);
		}
		
		if (insertIntoTempTable) {
			result += " INTO TABLE seedb_tmp_table_" + seqNum;
		}
		
		if (fromTempTable != null) {
			result += " FROM " + fromTempTable + " WHERE seedb_group_" + group + "=1";
		} else {
			if (!combined) {
				result += " FROM " + q1.fromClause;
				result += ((q1.whereClause != null && !q1.whereClause.isEmpty()) ? " WHERE " + 
							q1.whereClause : "");
			}
			else {
				result += " FROM " + q1.fromClause;
				// TODO: do I need a where extension?
			}
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
	
	/**
	 * Build unique names for the columns which store results of aggregates. 
	 * These are used in the SQL query when NOT using temp tables
	 * @param attrs
	 * @param aggregateFunctions
	 * @return
	 */
	public static List<String> applyAggregateFunctionsToAttributes(
			List<Attribute> attrs, List<List<String>> aggregateFunctions) {
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < attrs.size(); i++) {
			for (int j = 0; j < aggregateFunctions.get(i).size(); j++) {
				if (aggregateFunctions.get(i).get(j).equalsIgnoreCase("avg")) {
					continue;
				}
				list.add(aggregateFunctions.get(i).get(j) + "(" + attrs.get(i).name + ") as " + 
						(attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j));
			}
		}
		return list;
	}
	
	/**
	 * Build unique names for the columns which store results of aggregates. 
	 * These are used in the SQL query when using temp tables
	 * @param attrs
	 * @param aggregateFunctions
	 * @return
	 */
	public static List<String> getAggregateAttributeColumnNames(
			List<Attribute> attrs, List<List<String>> aggregateFunctions) {
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < attrs.size(); i++) {
			for (int j = 0; j < aggregateFunctions.get(i).size(); j++) { 
				if (aggregateFunctions.get(i).get(j).equalsIgnoreCase("avg")) {
					continue;
				}
				list.add("SUM" + "(" + (attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j) + ") as " + 
						(attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j));
			}
		}
		return list;
	}
	
	/**
	 * Build unique names for the columns which store results of aggregates
	 * @param attrs
	 * @param aggregateFunctions
	 * @return
	 */
	public static List<String> getColumnNamesForAggregateAttributes(
			List<Attribute> attrs, List<List<String>> aggregateFunctions) {
		List<String> list = Lists.newArrayList();
		for (int i = 0; i < attrs.size(); i++) {
			for (int j = 0; j < aggregateFunctions.get(i).size(); j++) {
				if (aggregateFunctions.get(i).get(j).equalsIgnoreCase("avg")) {
					continue;
				}
				list.add((attrs.get(i).name.equalsIgnoreCase("*") ? "ALL" : attrs.get(i).name)
						+ "__seedb__" + aggregateFunctions.get(i).get(j));
			}
		}
		return list;
	}
	
	/**
	 * Helper to get attribute names from attributes. Can probably use a lambda function instead
	 * @param attrs
	 * @return
	 */
	public static List<String> getAttributeNames(List<Attribute> attrs) {
		List<String> list = Lists.newArrayList();
		for (Attribute attr : attrs) {
			list.add(attr.name);
		}
		return list;
	}

	/**
	 * add an aggregate attribute and the associated aggregate functions to the view
	 * @param attribute
	 * @param aggFuncs
	 */
	public void addAggregateAttribute(Attribute attribute,
			List<String> aggFuncs) {
		aggregateAttributes.add(attribute);
		aggregateFunctions.add(aggFuncs);
	}
	
	
	/**
	 * serialize the group by attributes
	 * @return
	 */
	public String getSerializedGroupByAttributes() {
		List<String> attrs = getAttributeNames(groupByAttributes);
		Collections.sort(attrs);
		return Joiner.on("__").join(attrs);
	}

	/**
	 * create a deep  copy
	 * @param query
	 * @return
	 */
	public static DifferenceQuery deepCopy(DifferenceQuery query) {
		DifferenceQuery dq = new DifferenceQuery(query.op, query.inputQueries);
		dq.mergedQueries = query.mergedQueries;
		dq.limitClause = query.limitClause;
		dq.additionalWherePredicates = query.additionalWherePredicates;
		dq.selectAttributes = Utils.deepCopyList(query.selectAttributes);
		dq.aggregateAttributes = Utils.deepCopyList(query.aggregateAttributes);
		dq.groupByAttributes = Utils.deepCopyList(query.groupByAttributes);
		dq.aggregateFunctions = Utils.deepCopyListOfLists(query.aggregateFunctions);
		dq.derivedFrom = Utils.deepCopyList(query.derivedFrom);
		return dq;
	}
	
	/**
	 * identifier string
	 * @return
	 */
	public String getSerializedGroupByAndAggregateAttributes() {
		List<String> attrs = getAttributeNames(aggregateAttributes);
		Collections.sort(attrs);
		return getSerializedGroupByAttributes() + "____" + Joiner.on("__").join(attrs);
	}
	
	/**
	 * create an identifier
	 */
	public String toString() {
		return this.getSerializedGroupByAndAggregateAttributes();
	}		
}
