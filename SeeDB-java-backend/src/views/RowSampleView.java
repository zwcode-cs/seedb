package views;

import java.util.List;

import output.RowSampleOutputView;


import com.google.common.collect.Lists;

import common.ExperimentalSettings;
import common.Utils;
import common.ExperimentalSettings.DifferenceOperators;

public class RowSampleView implements View, RowSampleOutputView {
	public List<String> columnNames; // assume that both datasets have same columns
	public List<List<String>> rows1;
	public List<List<String>> rows2;
	
	public RowSampleView() {
		this.columnNames = Lists.newArrayList();
		this.rows1 = Lists.newArrayList();
		this.rows2 = Lists.newArrayList();
	}
	
	@Override
	public String serializeView() {
		String ret = "";
		ret += "diff_type:row_sample_diff;";
		ret += "columnNames:" + Utils.serializeList(columnNames) + ";";
		ret += "dataset_num:1;";
		ret += "rows:" + Utils.serializeListofLists(rows1) + ";";
		ret += "dataset_num:2;";
		ret += "rows:" + Utils.serializeListofLists(rows2) + ";";
		return ret;
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.DATA_SAMPLE;
	}

	@Override
	public List<String> getColumnNames() {
		return this.columnNames;
	}

	@Override
	public List<List<String>> getRows(int dataset) {
		if (dataset == 1) {
			return this.rows1;
		} else if (dataset == 2) {
			return this.rows2;
		} else {
			return null;
		}
	}
	
}
