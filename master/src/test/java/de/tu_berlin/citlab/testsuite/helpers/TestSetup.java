package de.tu_berlin.citlab.testsuite.helpers;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;

import de.tu_berlin.citlab.testsuite.mocks.MockTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * <h2>A final class that provides static methods for Test-Setup generation
 * in a CIT-Storm related environment. </h2>
 * <p>
 * <h3>The provided methods are classified in two groups:</h3>
 * <ul>
 * 	<li>Via the <em>setup-methods</em>, you are able to initialize the global</li>
 * 	setting variables. <br>
 * 	<b>There are two setup-methods: </b>
 * 	<ul>
 * 		<li><code>setupFields(..)</code> sets the input- and key-fields of the Storm environment.</li>
 * 		<li><code>setupTestParams(..)</code> sets more testing-dependent variables. </li>
 * 		<em>If you don't use one or both setup-methods, the default variable values will be used in the test-run.</em><br>
 * 	</ul>
 * 	<li>Via the <em>generate-methods</em>, you are able to generate CIT-Storm related datatypes, using Mockito Mock-ups, if needed.</li>
 * 	<b>There are several generate-method-types for each datatype:</b>
 * 	<ul>
 * 		<li><code>generate<em>Datatype</em>(..)</code> is an overloaded method for a given CIT-Storm related Datatype, 
 * 			which generates and returns exactly one value of <em>Datatype</em>.</li>
 * 		<li><code>generate<em>Datatype</em>Buffer(..)</code> is a method that generates and returns
 * 			 a <code>List<<em>Datatype</em>></code> of the given <em>Datatype</em>.</li>
 *  </ul>
 * </ul>
 * </p>
 * 
 * 
 * @author Constantin
 */
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
	
	
	public static Tuple generateTickTuple()
	{
		Tuple mockTickTuple = MockTuple.mockTickTuple();
		return mockTickTuple;
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
			
			if(i % tickInterval != 0){
				Tuple inputTuple = TestSetup.generateTuple(key, inputFields, keyFields, maxValueCount);
				tupleBuffer.add(inputTuple);
			}
			else{ //Add a Tick-Tuple to the tupleBuffer:
				Tuple tickTuple = TestSetup.generateTickTuple();
				tupleBuffer.add(tickTuple);
			}
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
