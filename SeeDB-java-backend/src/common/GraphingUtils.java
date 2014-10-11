package common;

import java.io.File;
import java.io.IOException;
import java.util.List;

import views.AggregateGroupByView;
import views.View;

public class GraphingUtils {
	public static void createFilesForGraphs(List<View> views) {
		for (View v : views) { // can select only the first k
			AggregateGroupByView v_ = (AggregateGroupByView) v;
			String viewFilePath = "/Users/manasi/Public/seedb_results/" + v_.getId() + ".txt";
			File viewFile = new File(viewFilePath);
			if (!viewFile.exists()) {
				try {
					viewFile.createNewFile();
				} catch (IOException e) {
					System.out.println("Couldn't create file: " + viewFile);
					e.printStackTrace();
				}
			} 
			String headerLine = v_.getGroupByAttributes() + ", selected_patients, all_patients";
			Utils.writeToFile(viewFile, headerLine);
			for (String k : v_.getResult().keySet()) {
				String toWrite = k + "," + v_.getResult().get(k).datasetValues[0].sum + "," + v_.getResult().get(k).datasetValues[1].sum;
				Utils.writeToFile(viewFile, toWrite);
			}
		}
	}

}
