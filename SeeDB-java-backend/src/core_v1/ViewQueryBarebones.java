package core_v1;

import java.util.Collections;
import java.util.List;

import utils.Constants;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ViewQueryBarebones {
	private List<String> aggregateAttributes;
	private List<String> groupByAttributes;
	
	public ViewQueryBarebones() {
		this.aggregateAttributes = Lists.newArrayList();
		this.groupByAttributes = Lists.newArrayList();
	}
	
	public ViewQueryBarebones clone() {
		ViewQueryBarebones res = new ViewQueryBarebones();
		res.aggregateAttributes.addAll(this.aggregateAttributes);
		res.groupByAttributes.addAll(this.groupByAttributes);
		return res;
	}
	
	public String toString() {
		Collections.sort(aggregateAttributes);
		Collections.sort(groupByAttributes);
		return Joiner.on("-").join(groupByAttributes) + Constants.spacer + 
				Constants.spacer + Joiner.on(Constants.spacer).join(
						aggregateAttributes);
	}

	public List<String> getGroupByAttributes() {
		return this.groupByAttributes;	
	}
	
	public List<String> getAggregateAttributes() {
		return this.aggregateAttributes;	
	}
}
