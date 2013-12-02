package unitTests;


import java.util.List;

import org.junit.Test;

import units.bucketstore.BucketStore;
import units.bucketstore.BucketStore.WinTypes;
import units.bucketstore.UDFWindowBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class Test_BucketStore extends DataPerformanceTest
{

	@Test
	public void test()
	{
		long startTime = System.currentTimeMillis();
		
	//Count-Based Input Window Test:
		Fields inputFields = new Fields("key", "value");
		UDFWindowBolt<List<Object>> windowBolt = new UDFWindowBolt<List<Object>>(inputFields, 100, WinTypes.CounterBased);
		//BucketStore<List<Object>, Values> bstore = new BucketStore<List<Object>, Values>(100, WinTypes.CounterBased);
		
		for(int n = 0 ; n < inputIterations ; n++){
			windowBolt.execute(sortKey, input);
//			bstore.sortInBucket(keyInputBuffer.get(n), valInputBuffer.get(n));
//			bstore.readyForExecution();
		}
		//bstore.sortInBucket(sortKey, input)
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
		
	
	//Check if BucketStore Output == Input:
		//bstore.readyForExecution()
	}

}
