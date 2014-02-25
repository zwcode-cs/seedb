package views;

import common.ExperimentalSettings.DifferenceOperators;

public interface View {
	public String serializeView();
	public DifferenceOperators getOperator();

}
