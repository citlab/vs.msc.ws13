package de.tu_berlin.citlab.testsuite.helpers;


import java.util.ArrayList;
import java.util.List;

import de.tu_berlin.citlab.testsuite.mocks.MockTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class TestSetup
{
	
/* Global Settings: */
/* ================ */
	
	private static int keyIDCount = 5;
	private static int keyListCount = 10;
	private static int maxValueCount = 10;
	private static int bufSize = 10000;
	
	private static Fields inputFields = new Fields("key", "value");
	private static Fields keyFields = new Fields("key1");
	private static List<Object> defaultKey = new ArrayList<Object>();
	
	
	public static void setupFields(Fields inputFields, Fields keyFields)
	{
		TestSetup.inputFields = inputFields;
		TestSetup.keyFields = keyFields;
	}
	
	public static void setupTestParams(int keyIDCount, int keyListCount, int maxValueCount, int bufSize)
	{
		TestSetup.keyIDCount = keyIDCount;
		TestSetup.keyListCount = keyListCount;
		TestSetup.maxValueCount = maxValueCount;
		TestSetup.bufSize = bufSize;
	}
	
/* Global Variables: */
/* ================= */
	
	private static List<Object> keyIDBuffer = new ArrayList<Object>();	
	
	
/* Public Testing-Methods: */
/* ======================= */
	
	public static List<Object> generateKey()
	{
		return TestSetup.generateKey(keyListCount);
	}
	
	public static List<Object> generateKey(int keyListCount)
	{
		int keyCombSize = (int) Math.round(Math.random() * keyListCount);
		ArrayList<Object> keyComb = new ArrayList<Object>(keyCombSize);
		
		//Initialization of Keys that can be combined in a List later:
		while(keyIDBuffer.size() < keyListCount){
			Integer randKey = (int) Math.round(Math.random() * 1000);
			keyIDBuffer.add(randKey);
		}
		
		//Combine some Keys to a Key-Combination, later used for a Value grouping:
		for(int n = 0 ; n < keyCombSize ; n++){
			int keySelector = (int) Math.round(Math.random() * (keyListCount-1));
			keyComb.add(keyIDBuffer.get(keySelector)); 
		}
		
		return keyComb;
	}
	
	
	public static List<List<Object>> generateKeyBuffer()
	{
		return TestSetup.generateKeyBuffer(bufSize, keyIDCount, keyListCount);
	}
	
	public static List<List<Object>> generateKeyBuffer(int bufSize,
													   int keyIDCount, int keyListCount)	
	{
		List<List<Object>> keyListBuffer = new ArrayList<List<Object>>(keyListCount);
		
		for(int n = 0 ; n < keyListCount ; n++){
			keyListBuffer.add(generateKey(keyListCount));
		}
		
		return keyListBuffer;
	}
	
	
	public static Tuple generateTuple()
	{
		return TestSetup.generateTuple(defaultKey);
	}
	
	public static Tuple generateTuple(List<Object> key)
	{
		return TestSetup.generateTuple(key, inputFields, keyFields, maxValueCount);
	}
	
	public static Tuple generateTuple(List<Object> key, 
									  Fields inputFields, Fields keyFields, 
									  int maxValueCount)
	{
		int valCount = (int) Math.round(Math.random() * maxValueCount);
		Values vals = new Values();
		
		for(int n = 0 ; n < valCount ; n++){
			vals.add(new String("Value "+ n));
		}
		Tuple mockTuple = MockTuple.mockTuple(key, vals, inputFields, keyFields);
		return mockTuple;
	}
	
	
	public static List<Tuple> generateTupleBuffer(int tickInterval)
	{
		return TestSetup.generateTupleBuffer(bufSize, keyIDCount, keyListCount, inputFields, keyFields, maxValueCount, tickInterval);
	}
	
	public static List<Tuple> generateTupleBuffer(int bufSize, 
												  int keyIDCount, int keyListCount,
												  Fields inputFields, Fields keyFields,
												  int maxValueCount, int tickInterval)
	{
		List<Tuple> tupleBuffer = new ArrayList<Tuple>(bufSize);
		List<List<Object>> keyListBuffer = generateKeyBuffer(bufSize, keyIDCount, keyListCount);
		
	//Buffer Generation:
		for(int i = 0 ; i <= bufSize ; i++){
			int randKeyComb = (int) Math.round((Math.random() * (keyListCount-1)));
			
			List<Object> key = keyListBuffer.get(randKeyComb);
			
			Tuple inputTuple = TestSetup.generateTuple(key, inputFields, keyFields, maxValueCount);
			tupleBuffer.add(inputTuple);
		}
		
		return tupleBuffer;
	}
	
	
	
	public static List<Values> generateValueList(Values... vals)
	{
		List<Values> valList = new ArrayList<Values>(vals.length);
		for(Values actVal : vals){
				valList.add(actVal);
		}
		
		return valList;
	}

}
