package de.tu_berlin.citlab.storm.units;


import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import backtype.storm.tuple.Values;


public class DataPerformanceTest
{
	
/* Global Constants: */
/* ================= */
	
	protected static final int keyCount = 5;
	protected static final int keyCombinCount = 20;
	protected static final int inputIterations = 1000;
	
	
/* Global Variables: */
/* ================= */
	
	protected static List<ArrayList<Object>> keyCombinations;
	protected static ArrayList<Object> keys;
	
	protected static List<ArrayList<Object>> keyInputBuffer;
	protected static List<Values> valInputBuffer;
	
	
	
/* Public JUnit-Methods: */
/* ===================== */
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		keyCombinations = new ArrayList<ArrayList<Object>>(keyCombinCount);
		keys = new ArrayList<Object>(keyCount);
		
		for(int n = 0 ; n < keyCombinCount ; n++){
			keyCombinations.add(generateKey());
		}
	
	//Buffer Generation:
		keyInputBuffer = new ArrayList<ArrayList<Object>>(inputIterations);
		valInputBuffer = new ArrayList<Values>(inputIterations);
		for(int i = 0 ; i <= inputIterations ; i++){
			int randKeyComb = (int) Math.round((Math.random() * (keyCombinCount-1)));
			keyInputBuffer.add(keyCombinations.get(randKeyComb));
			valInputBuffer.add(new Values("Value "+ i));
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
	}
	
	
	
/* Private JUnit-Methods: */
/* ===================== */
	
	static private ArrayList<Object> generateKey()
	{
		int keyCombSize = (int) Math.round(Math.random() * keyCount);
		ArrayList<Object> keyComb = new ArrayList<Object>(keyCombSize);
		
		//Initialization of Keys that can be combined in a List later:
		while(keys.size() < keyCount){
			Integer randKey = (int) Math.round(Math.random() * 1000);
			keys.add(randKey);
		}
		
		//Combine some Keys to a Key-Combination, later used for a Value grouping:
		for(int n = 0 ; n < keyCombSize ; n++){
			int keySelector = (int) Math.round(Math.random() * (keyCount-1));
			keyComb.add(keys.get(keySelector));
		}
		
		return keyComb;
	}

}
