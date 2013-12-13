package de.tu_berlin.citlab.testsuite.tests;


import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tu_berlin.citlab.testsuite.helpers.DataTest;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


//TODO: implement class with new Test-Suite
public class UDFBolt_PerfTests extends DataTest
{
	
/* Global Test Params: */
/* =================== */
	private static int keyIDCount = 5;
	private static int keyListCount = 10;
	private static int maxValueCount = 10;
	private static int iterations = 10000;
	private static int tickInterval = 10;
	
	private static Fields inputFields = new Fields("key", "value");
	private static Fields keyFields = new Fields("key1");
	
/* Global Buffers: */
/* =============== */
	private static List<Tuple> tupleBuffer;

	
//	UDFBoltMock winBolt;
//	
//	public Test_StormFS()
//	{
////Count-Based Input Window Test:
//		winBolt = new UDFBoltMock(inputFields, null, new IOperator(){
//			private static final long serialVersionUID = 1L;
//			public List<List<Object>> execute(List<List<Object>> param) {
//				// TODO Auto-generated method stub
//				return null;
//			}
//		}, new CountWindow<Tuple>(100, 50), keyFields);
//	}
	
	
	
/* JUnit 4 Test Setup: */
/* =================== */
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		DataTest.setupFields(inputFields, keyFields);
		DataTest.setupTestParams(keyIDCount, keyListCount, maxValueCount, iterations);
		
		tupleBuffer = DataTest.generateTupleBuffer(tickInterval);
	}

	
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
	}
	
	@Test
	public void test()
	{
		long startTime = System.currentTimeMillis();
		
	
		
		int n = 0;
		for(n = 0 ; n < iterations ; n++){
			Tuple actMockTuple = tupleBuffer.get(n);
//			winBolt.execute(actMockTuple);
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Iterations: "+ n);
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
	}

}
