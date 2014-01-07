package de.tu_berlin.citlab.testsuite.tests.skeletons;


import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.MockOutputCollector;


abstract public class OperatorTest
{
	private List<Tuple> inputTuples;
	private IOperator operator;
	
	
	@Before	
	public void initTestSetup()
	{
		inputTuples = this.generateInputValues();
		operator = this.initOperator(inputTuples);
	}
	
	
	@Test
	public void testOperator()
	{
		AssertionError failureTrace = null;
		
		System.out.println("================================================");
		System.out.println("=========== Starting Operator Test... ========== \n");
		
		
		long startTime = System.currentTimeMillis();
		
		OutputCollector outputCollector = MockOutputCollector.mockOutputCollector();
		operator.execute(inputTuples, outputCollector);
		List<Object> assertRes = assertOutput(inputTuples);
		
		try{//TODO: refactor.
//			assertTrue("Operator.execute(..) result is not equal to asserted Output! \n"+
//					   "Operator Result: "+ DebugPrinter.toString(result) +"\n"+
//					   "Asserted Output: "+ DebugPrinter.toString(assertRes), result.equals(assertRes));
			
			System.out.println("Success!");
		}
		catch (AssertionError e){
			System.out.println("Operator Test failed. For more infos, check the JUnit Failure Trace.");
			failureTrace = e;
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		
		System.out.println("\nSummary:");
		System.out.println("Number of Input-Values: "+ inputTuples.size());
		System.out.println("Time to execute input:"+ inputTimeDiff +" ms.");
		
		System.out.println("=========== Finished Operator Test! ===========");
		System.out.println("===============================================");
		
		if(failureTrace != null)
			throw failureTrace;
	}
	
	
	@After
	public void exitTestSetup()
	{
		inputTuples = null;
		operator = null;
	}
	
	
	
	abstract protected List<Tuple> generateInputValues();
	abstract protected IOperator initOperator(final List<Tuple> inputTuples);
	
	abstract protected List<Object> assertOutput(final List<Tuple> inputTuples);
}
