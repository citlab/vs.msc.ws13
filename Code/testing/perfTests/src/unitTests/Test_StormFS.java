package unitTests;


import java.util.List;

import org.junit.Test;

import stormFS.bolts.UDFBoltMock;
import stormFS.udf.IOperator;
import stormFS.window.CountWindow;
import backtype.storm.tuple.Tuple;


public class Test_StormFS extends DataTest
{

	@Test
	public void test()
	{
		long startTime = System.currentTimeMillis();
		
	//Count-Based Input Window Test:
		UDFBoltMock winBolt = new UDFBoltMock(inputFields, null, new IOperator(){
			private static final long serialVersionUID = 1L;
			public List<List<Object>> execute(List<List<Object>> param) {
				// TODO Auto-generated method stub
				return null;
			}
		}, new CountWindow<Tuple>(100, 50), keyFields);
		
		int n = 0;
		for(n = 0 ; n < inputIterations ; n++){
			Tuple actMockTuple = tupleInputBuffer.get(n);
			winBolt.execute(actMockTuple);
//			bstore.sortInBucket(keyInputBuffer.get(n), valInputBuffer.get(n));
//			bstore.readyForExecution();
		}
		//bstore.sortInBucket(sortKey, input)
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		System.out.println("Iterations: "+ n);
		System.out.println("Elapsed time for input: "+ inputTimeDiff);
		
	
	//Check if BucketStore Output == Input:
		//bstore.readyForExecution()
	}

}
