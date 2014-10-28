package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import main_memory_implementation.MainMemorySeeDB;

import org.junit.Test;

import com.google.common.collect.Lists;

import common.Utils;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.Backend;
import settings.ExperimentalSettings.MainMemoryPruningAlgorithm;
import views.AggregateGroupByView;
import views.View;

public class MainMemoryPaperExperiments {
	String[] datasets = {
			"/Users/manasi/Public/diabetes_50k.csv",
			"/Users/manasi/Public/bank-additional-full.csv",
			"/Users/manasi/Public/diabetic_data.csv"
	};
	
	int[] q_idxs = {
			4,
			2,
			4
	};
	
	int[] num_views = {
		24,
		70,
		80
	};
	
	int[] k_s = {
		1,
		5,
		10,
		15,
		20
	};
	
	double[] sample_sizes = {
			0.01,
			0.1,
			0.2,
			0.3,
			0.4,
			0.5,
			0.6,
			0.7,
			0.8,
			0.9
	};
	
	String[] q_values = {
			"[30-40)",
			"married",
			"[30-40)"
	};
	
	File logFile;
	
	public void setup(ExperimentalSettings settings) throws IOException {
		logFile = new File(settings.logFile);
		if (!logFile.exists()) {
			logFile.createNewFile();
		}
	}
	
	public List<String> readResultFile(String filename) throws Exception {
		List<String> res = Lists.newArrayList();
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = null;
		boolean first = true;
		while ((line = br.readLine()) != null) {
			if (first) {
				first = false;
				continue;
			}
			String parts[] = line.split(",");
			res.add(parts[0]);
		}
		return res;
	}
	
	public void postProcessResults(ExperimentalSettings settings, List<View> results, String dataFilename) throws Exception {
		boolean isRealResult = (settings.mainMemoryPruning == MainMemoryPruningAlgorithm.NONE);
		
		List<String> realResults = null;
		if (!isRealResult) {
			realResults = readResultFile(getFileName("no_pruning", dataFilename, settings.mainMemoryNumViewsToSelect)).subList(0, 
					settings.mainMemoryNumViewsToSelect);
		}
						
		int ctr = 0;
		for (int i = 0; i < results.size(); i++) {
			AggregateGroupByView v_ = (AggregateGroupByView) results.get(i);
			if (isRealResult) {
				Utils.writeToFile(logFile, v_.getId() + "," + v_.getUtility(settings.distanceMetric));
			} else {
				if (i < settings.mainMemoryNumViewsToSelect && realResults.contains(v_.getId())) {
					Utils.writeToFile(logFile, v_.getId() + "," + v_.getUtility(settings.distanceMetric) + " *");
					ctr++;
				} else {
					Utils.writeToFile(logFile, v_.getId() + "," + v_.getUtility(settings.distanceMetric));
				}
			}	
		}
		if (!isRealResult) {
			Utils.writeToFile(logFile, "Accuracy: " + ctr + "," + ctr*1.0/settings.mainMemoryNumViewsToSelect);
		}
	}
	
	public String getFileName(String algString, String dataset, int k) {
		if (algString.equalsIgnoreCase("no_pruning"))
			return "/Users/manasi/Public/top_k/" + algString + "_" + dataset + ".txt";
		return "/Users/manasi/Public/top_k/" + algString + "_" + dataset + "_" + k + ".txt";
	}
	
	@Test
	public void NoPruningTest() throws Exception {
		template(MainMemoryPruningAlgorithm.NONE);
	}
	
	//@Test
	public void RandomSamplePruningTest() throws Exception {
		double[] fractions = {0.1, 0.3, 0.5, 0.7, 0.9};
		templateWithExtraParam(MainMemoryPruningAlgorithm.RANDOM, fractions);
	}
	
	@Test
	public void Topk1PruningTest() throws Exception {
		// TODO: maybe we want to implement hoeffding bounds
		template(MainMemoryPruningAlgorithm.TOP_K1);
	}
	
	@Test
	public void Topk2PruningTest() throws Exception {
		// TODO: maybe we want to implement hoeffding bounds
		template(MainMemoryPruningAlgorithm.TOP_K2);
	}
	
	@Test
	public void Topk3PruningTest() throws Exception {
		// TODO: maybe we want to implement hoeffding bounds
		template(MainMemoryPruningAlgorithm.TOP_K3);
	}
	
	//@Test
	public void MAB1PruningTest() throws Exception {
		template(MainMemoryPruningAlgorithm.MAB1);
	}
		
	//@Test
	public void MAB2PruningTest() throws Exception {
		// TODO: maybe we want to vary rho
		template(MainMemoryPruningAlgorithm.MAB2);
	}

	//@Test
	public void MAB3PruningTest() throws Exception {
		template(MainMemoryPruningAlgorithm.MAB3);
	}
	
	@Test
	public void MAB4PruningTest() throws Exception {
		// TODO: maybe we want to vary # of phases
		template(MainMemoryPruningAlgorithm.MAB4);
	}
	
	@Test
	public void MAB5PruningTest() throws Exception {
		// TODO: maybe we want to vary # of phases
		template(MainMemoryPruningAlgorithm.MAB5);
	}

	public void template(MainMemoryPruningAlgorithm alg) throws Exception {
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.backend = Backend.MAIN_MEMORY;
		settings.normalizeDistributions = true;
		settings.mainMemoryPruning = alg;
		
		
		for (int i = 2; i < datasets.length; i++) {
			for (int j = 0; j < k_s.length; j++) {
				if (k_s[j] > num_views[i]) continue;
				settings.mainMemoryNumViewsToSelect = k_s[j];
				String[] parts = datasets[i].split("/");
				algorithmSpecificSetting(alg, settings, parts[parts.length - 1]);
				setup(settings);
				MainMemorySeeDB seedb = new MainMemorySeeDB();
				long startTime = System.currentTimeMillis();
				List<View> results = seedb.processFile(datasets[i], q_idxs[i], q_values[i], settings);
				Utils.writeToFile(logFile, "Time:" + (System.currentTimeMillis() - startTime));
				postProcessResults(settings, results, parts[parts.length - 1]);
				//System.out.println("K=" + k_s[j]);
			}
		}
	}
	
	public void templateWithExtraParam(MainMemoryPruningAlgorithm alg, double[] params) throws Exception {
		ExperimentalSettings settings = new ExperimentalSettings();
		settings.backend = Backend.MAIN_MEMORY;
		settings.normalizeDistributions = true;
		settings.mainMemoryPruning = alg;
			
		for (int i = 2; i < datasets.length; i++) {
			for (int j = 0; j < k_s.length; j++) {
				if (k_s[j] > num_views[i]) continue;
				settings.mainMemoryNumViewsToSelect = k_s[j];
				String[] parts = datasets[i].split("/");
				for (Double d : params) {
					algorithmSpecificSetting(alg, settings, parts[parts.length - 1], d);
					setup(settings);
					MainMemorySeeDB seedb = new MainMemorySeeDB();
					long startTime = System.currentTimeMillis();
					List<View> results = seedb.processFile(datasets[i], q_idxs[i], q_values[i], settings);
					Utils.writeToFile(logFile, "Time:" + (System.currentTimeMillis() - startTime));
					postProcessResults(settings, results, parts[parts.length - 1]);
				}
			}
		}
	}
	
	public void algorithmSpecificSetting(MainMemoryPruningAlgorithm alg, ExperimentalSettings settings, String dataset, Double extra) {
		switch(alg) {
		case RANDOM:
			settings.logFile = "random_" + dataset + "_" + settings.mainMemoryNumViewsToSelect + "_" + extra + ".txt";
			break;
		default:
			algorithmSpecificSetting(alg, settings, dataset);
		}
	}
	public void algorithmSpecificSetting(MainMemoryPruningAlgorithm alg, ExperimentalSettings settings, String dataset) {
		switch(alg) {
		case NONE:
			settings.logFile = getFileName("no_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case TOP_K1:
			settings.logFile = getFileName("top_k_1_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case TOP_K2:
			settings.logFile = getFileName("top_k_2_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case TOP_K3:
			settings.logFile = getFileName("top_k_3_pruning", dataset,
					settings.mainMemoryNumViewsToSelect);
			break;
		case MAB1:
			settings.logFile = getFileName("mab1_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case MAB2:
			settings.logFile = getFileName("mab2_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case MAB3:
			settings.logFile = getFileName("mab3_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case MAB4:
			settings.logFile = getFileName("mab4_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		case MAB5:
			settings.logFile = getFileName("mab5_pruning", dataset, 
					settings.mainMemoryNumViewsToSelect);
			break;
		}
	}

}
