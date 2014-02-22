package utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CommonOperationsTest {
	private List<String> list;
	
	public CommonOperationsTest() {
		setUp();
	}
	
	public void setUp() {
		this.list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");
		list.add("e");
	}
	
	public void printListOfLists(List<List<String>> result) {
		for (List<String> listOfString : result) {
			System.out.print("Unit: ");
			for (String str : listOfString) {
				System.out.print(str + ",");
			}
			System.out.println();
		}
	}

	@Test
	public void getCombinationsTest() {	
		List<List<String>> result = CommonOperations.getCombinations(3, list);
		printListOfLists(result);
	}
	
	@Test
	public void divideListTest() {
		List<List<String>> result = CommonOperations.divideList(list, 3);
		printListOfLists(result);
	}

}
