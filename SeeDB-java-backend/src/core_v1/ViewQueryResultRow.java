package core_v1;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import utils.Constants;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public class ViewQueryResultRow {
	private Map<String, String> groupByAttributeValuePairs;
	private Set<String> groupByAttributes;
	private int group;
	private Map<String, Double> aggregateAttributeValuePairs;
	private Set<String> aggregateAttributes;
	
	public ViewQueryResultRow() {
		groupByAttributeValuePairs = Maps.newHashMap();
		aggregateAttributeValuePairs = Maps.newHashMap();
		groupByAttributes = new TreeSet<String>();
		aggregateAttributes = new TreeSet<String>();
	}

	public ViewQueryResultRow(ViewQueryResultRow row) {
		this.group = row.group;
		this.groupByAttributeValuePairs = 
			copyMap(row.groupByAttributeValuePairs);
		this.aggregateAttributeValuePairs = 
			copyMap(row.aggregateAttributeValuePairs);
		this.aggregateAttributes = copySet(row.aggregateAttributes);
		this.groupByAttributes = copySet(row.groupByAttributes);
	}
	
	private static Set<String> copySet(Set<String> items) {
		Set<String> ret = new TreeSet<String>();
		for (String t : items) {
			ret.add(t);
		}
		return ret;
	}

	public static ViewQueryResultRow copyRowWithSpecificAggregate(
			ViewQueryResultRow row, 
			String aggregateAttribute) {
		ViewQueryResultRow newRow = new ViewQueryResultRow();
		newRow.group = row.group;
		newRow.groupByAttributeValuePairs = 
				copyMap(row.groupByAttributeValuePairs);
		newRow.groupByAttributes = copySet(row.groupByAttributes);
		newRow.addAggregateValue(aggregateAttribute, 
				row.getAggregateValue(aggregateAttribute));
		return newRow;
	}
	
	public Double getAggregateValue(String aggregateAttribute) {
		return this.aggregateAttributeValuePairs.get(aggregateAttribute);
	}
	
	public String getGroupByValue(String groupByAttribute) {
		return this.groupByAttributeValuePairs.get(groupByAttribute);
	}

	public void addAggregateValue(String attribute, Double value) {
		this.aggregateAttributes.add(attribute);
		this.aggregateAttributeValuePairs.put(attribute, value);
	}
	
	public void removeAggregateValue(String attribute) {
		this.aggregateAttributes.remove(attribute);
		this.aggregateAttributeValuePairs.remove(attribute);
	}
	
	public void addGroupByValue(String attribute, String value) {
		this.groupByAttributes.add(attribute);
		this.groupByAttributeValuePairs.put(attribute, value);
	}
	
	public void removeGroupByValue(String attribute) {
		this.groupByAttributes.remove(attribute);
		this.groupByAttributeValuePairs.remove(attribute);
	}

	private static <T> Map<String, T> copyMap(Map<String, T> map) {
		Map<String, T> result = Maps.newHashMap();
		for (String key : map.keySet()) {
			result.put(key, map.get(key));
		}
		return result;
	}

	public void setGroup(int i) {
		this.group = i;	
	}
	
	public boolean equals(Object o) {
		if ((o == null) || (o.getClass() != this.getClass())) return false;
		ViewQueryResultRow row = (ViewQueryResultRow) o;
		return row.toString().equals(this.toString());
	}

	public String getSerializedGroupBys() {
		return Joiner.on(Constants.spacer).join(groupByAttributes);
	}
	
	public String getSerializedGroupByValues() {
		return Joiner.on(Constants.spacer).join(
				this.groupByAttributeValuePairs.values()) + Constants.spacer +
				this.group;
	}
	
	public String toString() {
		return getSerializedGroupBys() + Constants.spacer + Constants.spacer +
				Joiner.on(Constants.spacer).join(aggregateAttributes);
	}

	public Set<String> getGroupByAttributes() {
		return this.groupByAttributes;
	}

	public Set<String> getAggregateAttributes() {
		return this.aggregateAttributes;
	}

	public Collection<String> getGroupByAttributeValues() {
		return this.groupByAttributeValuePairs.values();
	}

	public int getGroup() {
		return group;
	}

	public String getSerializedGroupByValues(List<String> combination) {
		String ret = "";
		for (String c : combination) {
			ret += Constants.spacer + this.groupByAttributeValuePairs.get(c);
		}
		ret += Constants.spacer + this.group;
		return ret.substring(1);
	}

	public void updateAggregateValue(String attribute, double value) {
		this.aggregateAttributeValuePairs.put(attribute, value);
	}
}
