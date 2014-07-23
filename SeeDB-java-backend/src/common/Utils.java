package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class Utils {
	public static void writeToFile(File file, String s) {
		if (file == null) {
			System.out.println(s);
			return;
		}
		try {
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(s + "\n");
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static <T> boolean listEqual(List<T> list1, List<T> list2) {
		if (((list1 == null) && (list2 != null)) || 
				((list2 == null) && (list1 != null))) return false;
		if (list1 == list2) return true;
		if (list1.size() != list2.size()) return false;
		for (int i = 0; i < list1.size(); i++) {
			if (!list1.get(i).equals(list2.get(i))) return false;
		}
		return true;
	}
	
	public static <T> String serializeList(List<T> list) {
		String ret = "{";
		for (T t : list) {
			ret += t.toString() + ",";
		}
		ret += "}";
		return ret;
	}
	
	public static <T> String serializeListofLists(List<List<T>> list) {
		String ret = "";
		for (List<T> t : list) {
			ret += serializeList(t) + ",";
		}
		return ret;
	}

	public static <T> void printList(List<T> result) {
		if (result == null) System.out.println(result);
		System.out.println("List:");
		for (T t : result) {
			System.out.println(t.toString() + ";");
		}
		System.out.println();
	}
	
	public static <T extends Comparable<T>> void combine(List<T> aggregateAttributes,
			List<T> aggregateAttributes2) {	
		aggregateAttributes.addAll(aggregateAttributes2);
		if (aggregateAttributes.isEmpty() || aggregateAttributes2.isEmpty()) {
			return;
		}
		Collections.sort(aggregateAttributes);
		T prev = aggregateAttributes.get(0);
		for (int i = 1; i < aggregateAttributes.size(); i++) {
			if (prev.equals(aggregateAttributes.get(i))) {
				aggregateAttributes.remove(i);
				i--;
				continue;
			}
			prev = aggregateAttributes.get(i);
		}
	}
	
	public static void combineAggregates(DifferenceQuery d1,
			DifferenceQuery d2) {	
		int d1_size = d1.aggregateAttributes.size();
		for (int i = 0; i < d2.aggregateAttributes.size(); i++) {
			boolean found = false;
			for (int j = 0; j < d1_size; j++) {
				if (d1.aggregateAttributes.get(j).equals(d2.aggregateAttributes.get(i))) {
					found = true;
					Utils.combine(d1.aggregateFunctions.get(j), d2.aggregateFunctions.get(i));
				}
			}
			if (!found) {
				d1.aggregateAttributes.add(d2.aggregateAttributes.get(i));
				d1.aggregateFunctions.add(d2.aggregateFunctions.get(i));
			}
		}
	}

	public static <T> List<T> deepCopyList(List<T> attrs) {
		List<T> copy = Lists.newArrayList();
		for (T a : attrs) {
			copy.add(a);
		}
		return copy;
	}

	public static <T> List<List<T>> deepCopyListOfLists(
			List<List<T>> aggregateFunctions) {
		List<List<T>> copy = Lists.newArrayList();
		for (List<T> s : aggregateFunctions) {
			copy.add(deepCopyList(s));
		}
		return copy;
	}

	public static <T> List<List<T>> getGroups(List<T> dimAttr,
			int gbSize) {
		List<List<T>> ret = Lists.newArrayList();
		if (gbSize == 1) {
			for (T t : dimAttr) {
				List<T> tmp = Lists.newArrayList();
				tmp.add(t);
				ret.add(tmp);
			}
		}
		return ret;
	}

}
