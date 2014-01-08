package de.tu_berlin.citlab.testsuite.tests.filterTests;


import java.util.List;

import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.helpers.TestSetup;
import de.tu_berlin.citlab.testsuite.tests.skeletons.UDFBoltTest;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


//TODO: implement class with new Test-Suite
public class BoltPerformanceTest extends UDFBoltTest
{

/* Global Test Params: */
/* =================== */
	
	private static int keyIDCount = 5;
	private static int keyListCount = 10;
	private static int maxValueCount = 10;
	private static int iterations = 10000;
	private static int tickInterval = 10;
	
	private static Fields inputFields = new Fields("key", "value");
	private static Fields outputFields = new Fields("key", "value");
	private static Fields keyFields = new Fields("key1");
	
	@Override
	protected List<Tuple> generateInputTuples()
	{
		TestSetup.setupFields(inputFields, keyFields);
		TestSetup.setupTestParams(keyIDCount, keyListCount, maxValueCount, iterations);
		
		List<Tuple> tupleBuffer = TestSetup.generateTupleBuffer(tickInterval);
		return tupleBuffer;
	}

	@Override
	protected UDFFields initUDFFields()
	{
		UDFFields udfFields = new UDFFields(inputFields, outputFields);
		return udfFields;
	}

	@Override
	protected IOperator initOperator(final List<Tuple> inputTuples)
	{
		Fields inputFields = new Fields("key", "value");
		
		FilterUDF filter= new FilterUDF(){
			private static final long	serialVersionUID	= 1L;
			private int count = 0;
			
			public Boolean evaluate(Tuple t)
			{
				//Test that inputTuples and Tuple t is the same for each iteration:
				if(t.equals(inputTuples.get(count))){
					count ++;
					System.out.println("Evaluation successful!");
					return true;
				}
				else return false;
			}
		};
		
		FilterOperator testFilterOp = new FilterOperator(inputFields, filter);
		return testFilterOp;
	}

	@Override
	protected Window<Tuple, List<Tuple>> initWindow()
	{
		return null;
	}

	@Override
	protected IKeyConfig initKeyConfig()
	{
		return null;
	}

	@Override
	protected List<Object> assertOutput(List<Tuple> inputTuples)
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
