package unitTests.helpers;


import java.util.ArrayList;
import java.util.List;

import unitTests.mocks.MockTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class DataTest
{
	
/* Global Settings: */
/* ================ */
	
	private static int keyIDCount = 5;
	private static int keyListCount = 10;
	private static int maxValueCount = 10;
	private static int bufSize = 10000;
	
	private static Fields inputFields = new Fields("key", "value");
	private static Fields keyFields = new Fields("key1");
	
	
	public static void setupFields(Fields inputFields, Fields keyFields)
	{
		DataTest.inputFields = inputFields;
		DataTest.keyFields = keyFields;
	}
	
	public static void setupTestParams(int keyIDCount, int keyListCount, int maxValueCount, int bufSize)
	{
		DataTest.keyIDCount = keyIDCount;
		DataTest.keyListCount = keyListCount;
		DataTest.maxValueCount = maxValueCount;
		DataTest.bufSize = bufSize;
	}
	
/* Global Variables: */
/* ================= */
	
	//protected static List<List<Object>> keyCombinations;
	private static List<Object> keyIDBuffer = new ArrayList<Object>();
	
	//protected static List<Tuple> tupleInputBuffer;
	
	
	
	
/* Public JUnit-Methods: */
/* ===================== */
	
//	@BeforeClass
//	public static void setUpBeforeClass() throws Exception
//	{
//		keyCombinations = new ArrayList<List<Object>>(keyCombinCount);
//		keyIDBuffer = new ArrayList<Object>(keyCount);
//		
//		for(int n = 0 ; n < keyCombinCount ; n++){
//			keyCombinations.add(generateKey());
//		}
//	
//	//Buffer Generation:
//		tupleInputBuffer = new ArrayList<Tuple>(inputIterations);
//		for(int i = 0 ; i <= inputIterations ; i++){
//			int randKeyComb = (int) Math.round((Math.random() * (keyCombinCount-1)));
////			keyInputBuffer.add(keyCombinations.get(randKeyComb));
////			valInputBuffer.add(new Values("Value "+ i));
//			
//			List<Object> key = keyCombinations.get(randKeyComb);
//			
//			Tuple inputTuple = DataTest.generateTuple(key, inputFields, keyFields);
//			tupleInputBuffer.add(inputTuple);
//		}
//	}
//
//	@AfterClass
//	public static void tearDownAfterClass() throws Exception
//	{
//	}
	
	
	
/* Public Testing-Methods: */
/* ======================= */
	
	public static List<Object> generateKey()
	{
		return DataTest.generateKey(keyListCount);
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
		return DataTest.generateKeyBuffer(bufSize, keyIDCount, keyListCount);
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
	
	
	public static Tuple generateTuple(List<Object> key)
	{
		return DataTest.generateTuple(key, inputFields, keyFields, maxValueCount);
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
		return DataTest.generateTupleBuffer(bufSize, keyIDCount, keyListCount, inputFields, keyFields, maxValueCount, tickInterval);
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
			
			Tuple inputTuple = DataTest.generateTuple(key, inputFields, keyFields, maxValueCount);
			tupleBuffer.add(inputTuple);
		}
		
		return tupleBuffer;
	}

}
