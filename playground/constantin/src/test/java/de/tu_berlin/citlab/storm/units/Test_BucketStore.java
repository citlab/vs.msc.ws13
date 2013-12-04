package de.tu_berlin.citlab.storm.units;


import java.util.List;
import org.junit.Test;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.windows.BucketStore;
import de.tu_berlin.citlab.storm.bolts.windows.BucketStore.WinTypes;


public class Test_BucketStore extends DataPerformanceTest
{

	@Test
	public void test()
	{
		long startTime = System.currentTimeMillis();
		
	//Count-Based Input Window Test:
		BucketStore<List<Object>, Values> bstore = new BucketStore<List<Object>, Values>(100, WinTypes.CounterBased);
		
		for(int n = 0 ; n < inputIterations ; n++){
			bstore.sortInBucket(keyInputBuffer.get(n), valInputBuffer.get(n));
			bstore.readyForExecution();
		}
		//bstore.sortInBucket(sortKey, input)
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
		
	
	//Check if BucketStore Output == Input:
		//bstore.readyForExecution()
	}

}
