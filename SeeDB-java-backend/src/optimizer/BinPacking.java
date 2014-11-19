package optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BinPacking {

	public class Pair {
		List<String> name;
		double logNumDistinctValues;
		
		public Pair() {
			name = Lists.newArrayList();
		}
		
		public String toString() {
			return name + ":" + logNumDistinctValues;
		}
	}
	
	public class Bin {
		List<Pair> items;
		double currSize = 0;
		
		public Bin() {
			items = Lists.newArrayList();
		}
	}
	
	public HashMap<Integer, Bin> binPack(double maxVolume, List<Pair> items) {
		HashMap<Integer, Bin> res = Maps.newHashMap();
		res.put(0, new Bin());
		int ctr = 0;
		for (Pair item : items) { // for every attribute
			boolean added = false;
			for (int i = 0; i <= ctr; i++) { // check all the currently open bins if they have room
				double tmp = item.logNumDistinctValues + res.get(i).currSize;
				if (tmp <= maxVolume) { // if there's room, stick it in
					res.get(i).items.add(item);
					res.get(i).currSize = tmp;
					added = true;
					break;
				}
			}
			// if doesn't fit in any open bin
			if (!added) {
				ctr++;
				res.put(ctr, new Bin());
				res.get(ctr).items.add(item);
				res.get(ctr).currSize = item.logNumDistinctValues;
			}
			/*
			for (Integer i: res.keySet()) {
				System.out.println(res.get(i).items);
			}
			System.out.println("----------------------");
			*/
		}
		return res;
	}
	
	public List<Pair> nameToPair(List<String> input) {
		List<Pair> l = Lists.newArrayList();
		for (String s : input) {
			Pair p = new Pair();
			p.name.add(s);
			p.logNumDistinctValues = Math.log(Integer.parseInt(s.split("_")[1]));
			l.add(p);
		}
		return l;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BinPacking bp = new BinPacking();
		double maxVolume = Math.log(102);
		List<String> dims = Lists.newArrayList();
		//String[] dimTemp = {"dim2_50", "dim3_5", "dim8_4", "dim4_25", "dim5_20", "dim9_15", "dim6_2", "dim7_4"};
		String[] dimTemp = {
				"dim1_1000", "dim2_500", "dim3_5000", "dim4_1000", "dim5_100", 
				"dim6_50", "dim7_500", "dim8_500", "dim9_500", "dim10_50",
				"dim11_1000", "dim12_2000", "dim13_200", "dim14_1000", "dim15_5",
				"dim16_100", "dim17_50", "dim18_5", "dim19_20", "dim20_20",
				"dim21_50", "dim22_1000", "dim23_5000", "dim24_5000", "dim25_20",
				"dim26_50", "dim27_2000", "dim28_20", "dim29_50", "dim30_2000",
				"dim31_500", "dim32_1000", "dim33_1000", "dim34_1000", "dim35_100",
				"dim36_500", "dim37_1000", "dim38_10", "dim39_200", "dim40_1000",
				"dim41_5", "dim42_1000", "dim43_100", "dim44_20", "dim45_200",
				"dim46_20", "dim47_100", "dim48_200", "dim49_5", "dim50_2000"};
		for (String s : dimTemp) {
			dims.add(s);
		}
		HashMap<Integer, Bin> res = bp.binPack(maxVolume, bp.nameToPair(dims));
		for (Integer i : res.keySet()) {
			System.out.print(i + ":");
			for (Pair item : res.get(i).items) {
				System.out.print(item.name);
			}
			System.out.println();
		}
	}

}
