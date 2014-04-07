package output;

import java.util.List;

public interface RowSampleOutputView extends OutputView {
	public List<String> getColumnNames();
	public List<List<String>> getRows(int dataset);
}
