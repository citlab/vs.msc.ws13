package unitTests;


import java.util.HashMap;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import stormCG.bolts.UDFWindowBoltMock;
import stormCG.bolts.windows.BucketStore;
import stormCG.bolts.windows.BucketStore.WinTypes;
import stormCG.udf.IBatchOperator;
import unitTests.helpers.DataTest;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class Test_StormCG
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
	
	
/* Global Testing Objects: */
/* ======================= */
	UDFWindowBoltMock winBolt;
	BucketStore<List<Object>, Values> bs;
	
	public Test_StormCG()
	{
	//Count-Based Input Window Test:
		this.winBolt = new UDFWindowBoltMock
					(inputFields, null, keyFields, 4, WinTypes.CounterBased, 
					new IBatchOperator()
		{
			private static final long serialVersionUID = 1L;
			public List<Values[]> execute_batch(
					HashMap<List<Object>, List<Values>> entryMap) {
				//System.out.println("Bucket Store has "+ entryMap.size() +" full Windows for batch processing!");
				return null;
			}			
		});
		
		this.bs = winBolt.get_bucketStore();
	}

	
	
	
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
	public void testPerformance()
	{
		long startTime = System.currentTimeMillis();
		
		int n;
		for(n = 0 ; n < iterations ; n++){
			Tuple actMockTuple = tupleBuffer.get(n);
			winBolt.execute(actMockTuple);
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Iterations: "+ n);
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
	}
	
	
	@Test
	public void testFunctionality()
	{
		Fields inputFields = new Fields("key", "value");
		Fields keyFields = new Fields("key1");
		DataTest.setupFields(inputFields, keyFields);
		
		List<Object> key1 = DataTest.generateKey();
		List<Object> key2 = DataTest.generateKey();
		
		boolean toggle = true;
		for(int n = 0 ; n < 20 ; n++){
			Tuple actTuple;
			if(toggle){
				actTuple = DataTest.generateTuple(key1);
				toggle = false;
			}
			else{
				actTuple = DataTest.generateTuple(key2);
				toggle = true;
			}
			System.out.println(actTuple.toString());
			winBolt.execute(actTuple);
			
			//assertTrue(bs.readyForExecution())
		}
	}

}
