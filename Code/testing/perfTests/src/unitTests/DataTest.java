package unitTests;


import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class DataTest
{
	
/* Global Constants: */
/* ================= */
	
	protected static final int keyCount = 5;
	protected static final int keyCombinCount = 10;
	protected static final int maxValueCount = 10;
	protected static final int inputIterations = 10000;
	
	protected static final Fields inputFields = new Fields("key", "value");
	protected static final Fields keyFields = new Fields("key1");
	
	
/* Global Variables: */
/* ================= */
	
	protected static List<List<Object>> keyCombinations;
	protected static List<Object> keys;
	
	protected static List<Tuple> tupleInputBuffer;
	
	
	
/* Public JUnit-Methods: */
/* ===================== */
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		keyCombinations = new ArrayList<List<Object>>(keyCombinCount);
		keys = new ArrayList<Object>(keyCount);
		
		for(int n = 0 ; n < keyCombinCount ; n++){
			keyCombinations.add(generateKey());
		}
	
	//Buffer Generation:
		tupleInputBuffer = new ArrayList<Tuple>(inputIterations);
		for(int i = 0 ; i <= inputIterations ; i++){
			int randKeyComb = (int) Math.round((Math.random() * (keyCombinCount-1)));
//			keyInputBuffer.add(keyCombinations.get(randKeyComb));
//			valInputBuffer.add(new Values("Value "+ i));
			
			List<Object> key = keyCombinations.get(randKeyComb);
			
			Tuple inputTuple = DataTest.generateTuple(key, inputFields, keyFields);
			tupleInputBuffer.add(inputTuple);
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
	}
	
	
	
/* Public Testing-Methods: */
/* ======================= */
	
	public static ArrayList<Object> generateKey()
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
	
	
	public static Tuple generateTuple(List<Object> key, Fields inputFields, Fields keyFields)
	{
		int valCount = (int) Math.round(Math.random() * maxValueCount);
		Values vals = new Values();
		
		for(int n = 0 ; n < valCount ; n++){
			vals.add(new String("Value "+ n));
		}
		Tuple mockTuple = MockTuple.mockTuple(key, vals, inputFields, keyFields);
		return mockTuple;
	}

}
