package de.tu_berlin.citlab.testsuite.tests.skeletons;


import java.util.List;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;


abstract public class OperatorTest
{
	private List<Values> inputValues;
	private IOperator operator;
	private Context context;
	
	
	@Before	
	public void initTestSetup()
	{
		inputValues = this.generateInputValues();
		operator = this.initOperator();
	}
	
	
	@Test
	public void testOperator()
	{
		AssertionError failureTrace = null;
		
		System.out.println("================================================");
		System.out.println("=========== Starting Operator Test... ========== \n");
		
		
		long startTime = System.currentTimeMillis();
		
		List<Values> result = operator.execute(inputValues, context);
		List<Values> assertRes = assertOutput(inputValues);
		
		try{
			assertTrue("Operator.execute(..) result is not equal to asserted Output! \n"+
					   "Operator Result: "+ DebugPrinter.toString(result) +"\n"+
					   "Asserted Output: "+ DebugPrinter.toString(assertRes), result.equals(assertRes));
			
			System.out.println("Success!");
		}
		catch (AssertionError e){
			System.out.println("Operator Test failed. For more infos, check the JUnit Failure Trace.");
			failureTrace = e;
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		
		System.out.println("\nSummary:");
		System.out.println("Number of Input-Values: "+ inputValues.size());
		System.out.println("Time to execute input:"+ inputTimeDiff +" ms.");
		
		System.out.println("=========== Finished Operator Test! ===========");
		System.out.println("===============================================");
		
		if(failureTrace != null)
			throw failureTrace;
	}
	
	
	@After
	public void exitTestSetup()
	{
		inputValues = null;
		operator = null;
		context = null;
	}
	
	
	
	abstract protected List<Values> generateInputValues();
	abstract protected Context initContext();
	abstract protected IOperator initOperator();
	
	abstract protected List<Values> assertOutput(List<Values> inputValues);
}
