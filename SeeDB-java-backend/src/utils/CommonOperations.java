package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

public class CommonOperations {
	public static List<List<String>> divideList(List<String> listToDivide, 
			int sizeOfDivision) {
		List<List<String>> resultList = new ArrayList<List<String>>();
		ArrayList<String> temp = new ArrayList<String>();
		for (int i = 0; i < listToDivide.size(); i++) {
			temp.add(listToDivide.get(i));
			if ((i + 1) % sizeOfDivision == 0) {
				resultList.add(temp);
				temp = new ArrayList<String>();
			}
		}
		if (temp.size() > 0) {
			resultList.add(temp);
		}
		return resultList;
	}
	
	public static List<List<String>> getCombinations(int sizeOfCombination, List<String> items) {
		if ((sizeOfCombination == 0) || (sizeOfCombination > items.size())) {
			return new ArrayList<List<String>>();
		}
		Collections.sort(items);
		if (sizeOfCombination == 1) {
			ArrayList<List<String>> result = new ArrayList<List<String>>();
			for (String item: items) {
				ArrayList<String> l = new ArrayList<String>();
				l.add(item);
				result.add(l);
			}
			return result;
		} 
		else {
			List<List<String>> result = new ArrayList<List<String>>();
			for (int i = 0; i < items.size(); i++) {
				List<List<String>> temp = getCombinations(sizeOfCombination - 1
						, items.subList(i+1, items.size()));
				for (List<String> combination: temp) {
					combination.add(0, items.get(i));
				}
				result.addAll(temp);
			}
			return result;
		}
	}

	// rewrite
	public static List<List<String>> getCombinationsMaxSize(
			int sizeOfCombination, List<String> items ) {
		Collections.sort(items);
		ArrayList<List<String>> result = new ArrayList<List<String>>();
		for (int i = 1; i <= sizeOfCombination; i++) {
			result.addAll(getCombinations(i, items));
		}
		return result;
	}

	public static <T> List<T> setToList(Set<T> input) {
		List<T> result = Lists.newArrayList();
		for (T t : input) {
			result.add(t);
		}
		return result;
	}

}
