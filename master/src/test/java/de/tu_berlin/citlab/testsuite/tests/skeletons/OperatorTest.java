package de.tu_berlin.citlab.testsuite.tests.skeletons;


import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;


abstract public class OperatorTest
{
	private List<Tuple> inputTuples;
	private IOperator operator;
	
	
	@Before	
	public void initTestSetup()
	{
		inputTuples = this.generateInputValues();
		if(inputTuples == null)
			throw new NullPointerException("InputTuples must not be null! \n Return them in generateInputValues().");
		
		operator = this.initOperator(inputTuples);
		if(operator == null)
			throw new NullPointerException("Operator must not be null! Return it in initOperator(..)");
	}
	
	
	@Test
	public void testOperator()
	{
		AssertionError failureTrace = null;
		
		System.out.println("================================================");
		System.out.println("=========== Starting Operator Test... ========== \n");
		
		
		long startTime = System.currentTimeMillis();
		
		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		operator.execute(inputTuples, outputCollector);
		List<List<Object>> outputVals = OutputCollectorMock.output;
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
		System.out.println("Number of Ouput-Values: "+ outputVals.size());
		System.out.println("Time to execute input:"+ inputTimeDiff +" ms. \n");
		
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
