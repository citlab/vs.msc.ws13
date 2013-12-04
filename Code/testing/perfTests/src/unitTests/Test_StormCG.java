package unitTests;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import stormCG.bolts.UDFWindowBoltMock;
import stormCG.bolts.windows.BucketStore;
import stormCG.bolts.windows.BucketStore.WinTypes;
import stormCG.udf.IBatchOperator;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class Test_StormCG extends DataTest
{

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
	
	@Test
	public void testPerformance()
	{
		long startTime = System.currentTimeMillis();
		
		int n;
		for(n = 0 ; n < inputIterations ; n++){
			Tuple actMockTuple = tupleInputBuffer.get(n);
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
		
		List<Object> key1 = DataTest.generateKey();
		List<Object> key2 = DataTest.generateKey();
		
		boolean toggle = true;
		for(int n = 0 ; n < 20 ; n++){
			Tuple actTuple;
			if(toggle){
				actTuple = DataTest.generateTuple(key1, inputFields, keyFields);
				toggle = false;
			}
			else{
				actTuple = DataTest.generateTuple(key2, inputFields, keyFields);
				toggle = true;
			}
			System.out.println(actTuple.toString());
			winBolt.execute(actTuple);
			
			//assertTrue(bs.readyForExecution())
		}
	}

}
