package main_memory_implementation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import settings.ExperimentalSettings;
import settings.ExperimentalSettings.MainMemoryPruningAlgorithm;
import utils.Constants.AggregateFunctions;
import views.AggregateGroupByView;
import views.AggregateView;
import views.View;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import common.Attribute;
import common.DifferenceQuery;
import common.GraphingUtils;

public class MainMemorySeeDB {
	
	BufferedReader br; // buffered read to read from file
	List<String[]> values; // values of dims
	Map<Integer, String> dim_idx = Maps.newHashMap(); // map from index to name of dimension column
	Map<Integer, String> measure_idx = Maps.newHashMap(); // map from index to name of measure column
	Map<String, Integer> dim_idx_reverse = Maps.newHashMap(); // map from name of dimension column to idx
	Map<String, Integer> measure_idx_reverse = Maps.newHashMap(); // map from name of measure column to idx
	Map<String, View> views = Maps.newHashMap(); // map from view id to view
	Map<String, ViewMetadata> viewMetadata = Maps.newHashMap();
	List<ViewMetadata> activeViews = Lists.newArrayList();	// views that have not yet been pruned
	List<ViewMetadata> viewsInRunning = Lists.newArrayList(); // USED BY MAB4;
	List<ViewMetadata> acceptedViews = Lists.newArrayList(); // USED BY MAB4
	int numRowsRead = 0; // number of rows that have been read
	
	ExperimentalSettings settings;
	int value_idx = -1;
	
	private class ViewMetadata {
		String key;
		int dim_idx;
		int measure_idx;
		int numSamples = 0;
		double utility;
		double utilityMean;
		double utilityVarianceProxy;
		double utilityLowerBound;
		double utilityUpperBound;
		
		public ViewMetadata(String key, int dim_idx, int measure_idx) {
			this.key = key;
			this.dim_idx = dim_idx;
			this.measure_idx = measure_idx;
		}
		
		public String toString() {
			return key + ":" + utility + " (" + utilityLowerBound + "," + utilityUpperBound + ")";
		}
	}
	
	public void getSchemaAndCreateViewStubs(String line, int q_idx) {
		List<String> dims = Lists.newArrayList();
		List<String> measures = Lists.newArrayList();
		// read in schema
		String parts[] = line.split(",");
		for (int i = 0 ; i < parts.length; i++) {
			if (i == q_idx) {
				continue;
			}
			String part = parts[i].trim();
			if (part.startsWith("dim")) {
				dims.add(part);
				dim_idx.put(i, part);
				dim_idx_reverse.put(part,  i);
			} else if (part.startsWith("measure")) {
				measures.add(part);
				measure_idx.put(i, part);
				measure_idx_reverse.put(part, i);
			}
		}
	   
		// create the views
		for (String dim : dims) {
			for (String measure : measures) {
				DifferenceQuery dq = new DifferenceQuery();
				dq.groupByAttributes.add(new Attribute(dim));
				dq.aggregateAttributes.add(new Attribute(measure));
				String key = dim + "__" + measure;
				views.put(key, new AggregateGroupByView(dq));
				ViewMetadata vm = new ViewMetadata(key, dim_idx_reverse.get(dim), measure_idx_reverse.get(measure));
				viewMetadata.put(dim + "__" + measure, vm);
				activeViews.add(vm);
			}
		}
	}
	
	public boolean initialize(String filename, final ExperimentalSettings settings, int q_idx) {
		this.settings = settings;
		if (settings.mainMemoryPruning == MainMemoryPruningAlgorithm.MAB1) {
			settings.mainMemoryUCB1Rho = 2;
		} else if (settings.mainMemoryPruning == MainMemoryPruningAlgorithm.MAB2) {
			settings.mainMemoryUCB1Rho = 0.2;
		}
		
		try {
			br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			if (line == null) return false;
			 
			// read first line to schema
			getSchemaAndCreateViewStubs(line, q_idx);
			if (settings.mainMemoryPruning == MainMemoryPruningAlgorithm.MAB4 || 
				settings.mainMemoryPruning == MainMemoryPruningAlgorithm.MAB5) {
				for (ViewMetadata vm : activeViews) {
					viewsInRunning.add(vm);
				}
				settings.mainMemoryPhased = true;
				settings.mainMemoryNumPhases = views.size() - 1;
			}
				
			if (!settings.mainMemoryReadFromFile) {
				// read full file into memory
				while ((line = br.readLine()) != null) {
					String parts[] = line.split(",");
					for (int i = 0; i < parts.length; i++) {
					   parts[i] = parts[i].trim();
					}
					values.add(parts);	
				}
			}
		} catch ( Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public String[] getNextValues() {
		if (settings.mainMemoryReadFromFile) {
			String line = null;
			String parts[] = null;
			try {
				line = br.readLine();
				if (line != null) {
					parts = line.split(",");
					for (int i = 0; i < parts.length; i++) {
					   parts[i] = parts[i].trim();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return parts;
		} else {
			value_idx++;
			return (value_idx >= values.size()) ? null : values.get(value_idx);
		}
	}
	
	public List<String> getViewsToUpdate() {
		List<String> ret = Lists.newArrayList();
		double minLowerBound = Double.MAX_VALUE;
		
		switch (settings.mainMemoryPruning) {
		case NONE:
			for (ViewMetadata vm : activeViews) {
				ret.add(vm.key);
			}
			break;
			
		case RANDOM:
			for (ViewMetadata vm : activeViews) {
				if (Math.random() < settings.mainMemoryRandomSamplingRate) {
					ret.add(vm.key);
				}
			}
			break;
		
		case TOP_K1:
			// check if any can be pruned
			if (numRowsRead <= settings.mainMemoryMinRows) {
				for (ViewMetadata vm : activeViews) {
					ret.add(vm.key);
				}
				break;
			}
			
			// sort active views in order of upper bound
			Collections.sort(activeViews, new Comparator<ViewMetadata>() {
		           @Override
					public int compare(ViewMetadata arg0, ViewMetadata arg1) {
						return arg0.utilityUpperBound - arg1.utilityUpperBound >= 0 ? -1 : 1;
					}
				});
			
			// find least lower bound in the top-k
			for (int i = 0; i < settings.mainMemoryNumViewsToSelect; i++) {
				if (i > activeViews.size()) break;
				
				if (minLowerBound >= activeViews.get(i).utilityLowerBound) {
					minLowerBound = activeViews.get(i).utilityLowerBound;
				}
				ret.add(activeViews.get(i).key);
			}
			
			for (int i = settings.mainMemoryNumViewsToSelect; i < activeViews.size(); i++) {
				if (activeViews.get(i).utilityUpperBound < minLowerBound) {
					//System.out.println(activeViews.get(i).key + " removed. Phase:" +
					//		(numRowsRead / (settings.mainMemoryNumRows / settings.mainMemoryNumPhases)));
					activeViews.remove(i);
					i--;
					continue;
				}
				ret.add(activeViews.get(i).key);
			}
			break;
		case TOP_K2:
		case TOP_K3:
			if (numRowsRead <= settings.mainMemoryMinRows) {
				for (ViewMetadata vm : activeViews) {
					ret.add(vm.key);
				}
				break;
			}

			// if not at end of a phase, return all
			if (numRowsRead % (settings.mainMemoryNumRows / settings.mainMemoryNumPhases) == 0) {
				// sort active views in order of upper bound
				Collections.sort(activeViews, new Comparator<ViewMetadata>() {
			           @Override
						public int compare(ViewMetadata arg0, ViewMetadata arg1) {
							return arg0.utilityUpperBound - arg1.utilityUpperBound >= 0 ? -1 : 1;
						}
					});
				
				// find least lower bound in the top-k
				for (int i = 0; i < settings.mainMemoryNumViewsToSelect; i++) {
					if (i > activeViews.size()) break;
					
					if (minLowerBound >= activeViews.get(i).utilityLowerBound) {
						minLowerBound = activeViews.get(i).utilityLowerBound;
					}
				}
				
				for (int i = settings.mainMemoryNumViewsToSelect; i < activeViews.size(); i++) {
					if (activeViews.get(i).utilityUpperBound < minLowerBound) {
						//System.out.println(activeViews.get(i).key + " removed. Phase:" +
						//		(numRowsRead / (settings.mainMemoryNumRows / settings.mainMemoryNumPhases)));
						activeViews.remove(i);
						i--;
						continue;
					}
				}
			}
			for (ViewMetadata vm : activeViews) {
				ret.add(vm.key);
			}
			break;
		case MAB1:
		case MAB2:
		case MAB3:
			if (numRowsRead <= settings.mainMemoryMinRows) {
				ret.add(activeViews.get((int) Math.floor(Math.random() * activeViews.size())).key);
				break;
			}
			double maxUCB = Double.MIN_VALUE;
			ViewMetadata vm_ = null;
			for (ViewMetadata vm : activeViews) {
				long horizon = numRowsRead;
				if (settings.mainMemoryPruning == MainMemoryPruningAlgorithm.MAB3) {
					horizon = settings.mainMemoryNumRows;
				}
				double ucb = vm.utilityMean + Math.sqrt(
						settings.mainMemoryUCB1Rho*Math.log(horizon) / vm.numSamples);
				if (ucb > maxUCB) {
					maxUCB = ucb;
					vm_ = vm;
				}
			}
			ret.add(vm_.key);
			break;
			
		case MAB4:
		case MAB5:
			if (acceptedViews.size() >= settings.mainMemoryNumViewsToSelect) {
				for (ViewMetadata vm : acceptedViews) {
					//System.out.println(vm.key);
					ret.add(vm.key);
				}
				break;
			}
			// if not at end of a phase, return all
			if (numRowsRead % (settings.mainMemoryNumRows / settings.mainMemoryNumPhases) == 0) {
				// if at end of phase, accept or reject one and keep going
				if (viewsInRunning.size() > settings.mainMemoryNumViewsToSelect) {
					Collections.sort(viewsInRunning, new Comparator<ViewMetadata>() {
				           @Override
							public int compare(ViewMetadata arg0, ViewMetadata arg1) {
								return arg0.utilityMean - arg1.utilityMean >= 0 ? -1 : 1;
							}
						});
					
					double best_distance_in_top_k = viewsInRunning.get(0).utilityMean - 
							viewsInRunning.get(settings.mainMemoryNumViewsToSelect).utilityMean;
					double best_distance_not_in_top_k = viewsInRunning.get(settings.mainMemoryNumViewsToSelect - 1).utilityMean - 
							viewsInRunning.get(viewsInRunning.size()-1).utilityMean;
					if (best_distance_in_top_k >= best_distance_not_in_top_k) {
						acceptedViews.add(viewsInRunning.get(0));
						viewsInRunning.remove(0);
						//System.out.println("remove best:" + viewsInRunning.get(0).key + "," + viewsInRunning.get(0).utilityMean);
					} else {
						ViewMetadata vm = viewsInRunning.get(viewsInRunning.size()-1);
						viewsInRunning.remove(viewsInRunning.size()-1);
						activeViews.remove(vm); // double check on this
						//System.out.println("remove worst:" + vm.key + "," + vm.utilityMean);
					}
				}
			}
			for (ViewMetadata vm : activeViews) {
				ret.add(vm.key);
			}
			break;
		}
		return ret;
	}
	
	public void updateView(String key, String query, String actual, String  gbValue, Double measureValue) {
		if (!views.containsKey(key)) {
			System.out.println("Something weird: all views should be precomputed");
			return;
		}
		AggregateGroupByView view = (AggregateGroupByView) views.get(key);
		if (query.equalsIgnoreCase(actual)) { // check if the row satisfies the query
			view.addAggregateValue(gbValue, AggregateFunctions.COUNT, 1.0, 1);
		    view.addAggregateValue(gbValue, AggregateFunctions.SUM, measureValue, 1);
		} 
		view.addAggregateValue(gbValue, AggregateFunctions.COUNT, 1.0, 2);
		view.addAggregateValue(gbValue, AggregateFunctions.SUM, measureValue, 2); 
	}
	
	public void updateViewMetadata(String key) {
		if (!viewMetadata.containsKey(key) || !views.containsKey(key)) {
			System.out.println("Something weird: all views and metadatas should be precomputed");
			return;
		}
		ViewMetadata vm = (ViewMetadata) viewMetadata.get(key);
		View view = (View) views.get(key);
		double[] res;
		double ci_half;
		vm.numSamples++;
		vm.utility = view.getUtility(settings.distanceMetric, settings.normalizeDistributions);
		
		// do any updating based on pruning type
		switch (settings.mainMemoryPruning) {
		case NONE:
			break;
			
		case RANDOM:
			break;
			
		case TOP_K1:
			// update mean and variance
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			vm.utilityVarianceProxy = res[1];
			ci_half = 1.96 * Math.sqrt(vm.utilityVarianceProxy / vm.numSamples); // 95% CI
			vm.utilityLowerBound = vm.utilityMean - ci_half;
			vm.utilityUpperBound = vm.utilityMean + ci_half;
			break;
			
		case TOP_K2:
			// update mean and variance
			if (numRowsRead % (settings.mainMemoryNumRows / settings.mainMemoryNumPhases) == 1) { 
				// if this is the first row in the new phase, start anew
				vm.numSamples = 1;
			}
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			vm.utilityVarianceProxy = res[1];
			ci_half = 1.96 * Math.sqrt(vm.utilityVarianceProxy / (vm.numSamples)); // 95% CI
			//ci_half = Math.sqrt(2*Math.log(settings.mainMemoryNumRows) / vm.numSamples);
			vm.utilityLowerBound = vm.utilityMean - ci_half;
			vm.utilityUpperBound = vm.utilityMean + ci_half;
			break;
		case TOP_K3:
			// update mean and variance
			if (numRowsRead % (settings.mainMemoryNumRows / settings.mainMemoryNumPhases) == 1) { 
				// if this is the first row in the new phase, start anew
				vm.numSamples = 1;
			}
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			vm.utilityVarianceProxy = res[1];
			ci_half = 1.96 * Math.sqrt(vm.utilityVarianceProxy / (vm.numSamples * numRowsRead / settings.mainMemoryNumRows)); // 95% CI
			//ci_half = Math.sqrt(2*Math.log(settings.mainMemoryNumRows) / vm.numSamples);
			vm.utilityLowerBound = vm.utilityMean - ci_half;
			vm.utilityUpperBound = vm.utilityMean + ci_half;
			break;
			
		case MAB1:
		case MAB2:
		case MAB3:
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			vm.utilityVarianceProxy = res[1];
			break;
		case MAB4:
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			break;
		case MAB5:
			if (numRowsRead % (settings.mainMemoryNumRows / settings.mainMemoryNumPhases) == 1) { 
				// if this is the first row in the new phase, start anew
				vm.numSamples = 1;
			}
			res = updateMeanAndStdDev(vm.utility, vm.utilityMean, vm.numSamples, vm.utilityVarianceProxy);
			vm.utilityMean = res[0];
			break;
		}
	}
	
	public double[] updateMeanAndStdDev(double newNum, double oldMean, int numSamples, double oldVariance) {
		double[] res = {0,0};
		if (numSamples == 1) {
			res[0] = newNum;
		} else {
			res[0] = (oldMean * (numSamples -1) + newNum)/numSamples; // alternatively: mean = mean + (x - mean)/n
			res[1] = oldVariance + (newNum - oldMean) * (newNum - res[0]);
		}
		return res;
	}
	
	// main function
	public List<View> processFile(String filename, int q_idx, String q_value, final ExperimentalSettings settings) {
		initialize(filename, settings, q_idx);
		String values[] = null;
		while ((values = getNextValues()) != null) {
			numRowsRead++;
			for (String key : getViewsToUpdate()) {
				ViewMetadata vm = viewMetadata.get(key);
				updateView(key, q_value, values[q_idx], values[vm.dim_idx], 
						Double.parseDouble(values[vm.measure_idx].trim()));
				updateViewMetadata(key);
			}  
		}
		
		return postProcessViews();
	}
	
	public List<View> postProcessViews() {
		// process and sort the views
		List<View> result_views = Lists.newArrayList();
		for (ViewMetadata vm  : activeViews) {
			result_views.add(views.get(vm.key));
		}

		Collections.sort(result_views, new Comparator<View>() {
	           @Override
				public int compare(View arg0, View arg1) {
					if (arg0 instanceof AggregateView || arg0 instanceof AggregateGroupByView) {
						return arg0.getUtility(settings.distanceMetric, settings.normalizeDistributions) - 
								arg1.getUtility(settings.distanceMetric, settings.normalizeDistributions) >= 0 ? -1 : 1;
					} else {
						return 1;
					}
				}
			});
			
		if (settings.makeGraphs) {
			GraphingUtils.createFilesForGraphs(result_views);
		}	
		return result_views;	
	}
}
