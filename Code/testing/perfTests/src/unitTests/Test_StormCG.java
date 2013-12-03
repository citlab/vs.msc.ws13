package unitTests;


import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import stormCG.bolts.UDFWindowBoltMock;
import stormCG.bolts.windows.BucketStore.WinTypes;
import stormCG.udf.IBatchOperator;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class Test_StormCG extends DataPerformanceTest
{

	@Test
	public void test()
	{
		long startTime = System.currentTimeMillis();
		
	//Count-Based Input Window Test:
		UDFWindowBoltMock<List<Object>> winBolt = new UDFWindowBoltMock<List<Object>>
											(inputFields, null, 100, WinTypes.CounterBased, 
											new IBatchOperator<List<Object>>()
			{
				private static final long serialVersionUID = 1L;
					public List<Values[]> execute_batch(
							HashMap<List<Object>, List<Values>> entryMap) {
						// TODO Auto-generated method stub
						return null;
				}
				public List<Object> sortBy_winKey(Values param) {
					// TODO Auto-generated method stub
					return null;
				}			
		});
		int n;
		for(n = 0 ; n < inputIterations ; n++){
			Tuple actMockTuple = tupleInputBuffer.get(n);
			winBolt.execute(actMockTuple);
//			bstore.sortInBucket(keyInputBuffer.get(n), valInputBuffer.get(n));
//			bstore.readyForExecution();
		}
		System.out.println(n +" Iterations made.");
		//bstore.sortInBucket(sortKey, input)
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
		
	
	//Check if BucketStore Output == Input:
		//bstore.readyForExecution()
	}

}
