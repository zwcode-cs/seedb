package output;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public interface AggregateGroupByOutputView extends OutputView {
	public Set<String> getAggregateAttributeIndex();
	public List<String> getGroupByAttributes();
	public HashMap<String, List<List<Double>>> getResult();
}
