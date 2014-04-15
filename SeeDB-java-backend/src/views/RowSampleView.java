package views;

import java.util.List;
import com.google.common.collect.Lists;
import common.ExperimentalSettings.DifferenceOperators;

public class RowSampleView implements View {
	public List<String> columnNames; // assume that both datasets have same columns
	public List<List<String>> rows1;
	public List<List<String>> rows2;
	
	public RowSampleView() {
		this.columnNames = Lists.newArrayList();
		this.rows1 = Lists.newArrayList();
		this.rows2 = Lists.newArrayList();
	}

	@Override
	public DifferenceOperators getOperator() {
		return DifferenceOperators.DATA_SAMPLE;
	}

	public List<String> getColumnNames() {
		return this.columnNames;
	}

	public List<List<List<String>>> getRows() {
		return Lists.newArrayList(this.rows1, this.rows2);
	}
	
}
