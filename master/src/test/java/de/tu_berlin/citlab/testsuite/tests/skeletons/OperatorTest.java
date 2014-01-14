package de.tu_berlin.citlab.testsuite.tests.skeletons;


import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;


abstract public class OperatorTest
{
	private final static String TAG = "OperatorTest";
	
	private List<Tuple> inputTuples;
	private IOperator operator;
	
	
	@Before	
	public void initTestSetup()
	{
		this.configureDebugLogger();
		
		
		DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, DebugLogger.print_Header("Initializing Operator-Test Setup...", '-'));
		
		try{
			inputTuples = this.generateInputValues();
			DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, "Input-Tuples are: ", DebugPrinter.toTupleListString(inputTuples));
		}
		catch (NullPointerException e){
			String errorMsg = "InputTuples must not be null! \n Return them in generateInputValues().";
			DebugLogger.printAndLog_Error(TAG, errorMsg, e.toString());
			throw new NullPointerException(errorMsg);
			
		}			
		try{
			operator = this.initOperator(inputTuples);
			DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, "Operator successfully initialized.");
		}
		catch (NullPointerException e){
			String errorMsg = "Operator must not be null! Return it in initOperator(..)";
			DebugLogger.printAndLog_Error(TAG, errorMsg, e.toString());
			throw new NullPointerException(errorMsg);
		}
			
	}
	
	
	@Test
	public void testOperator()
	{
		AssertionError failureTrace = null;
		
		DebugLogger.printAndLog_Message(LoD.BASIC, TAG, DebugLogger.print_Header("Starting Operator Test...", '='));
		
		long startTime = System.currentTimeMillis();
		
		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		operator.execute(inputTuples, outputCollector);
		List<List<Object>> outputVals = OutputCollectorMock.output;
		
		List<Object> assertRes = assertOutput(inputTuples);
		
		try{//TODO: refactor.
//			assertTrue("Operator.execute(..) result is not equal to asserted Output! \n"+
//					   "Operator Result: "+ DebugPrinter.toString(result) +"\n"+
//					   "Asserted Output: "+ DebugPrinter.toString(assertRes), result.equals(assertRes));
			
			DebugLogger.printAndLog_Message(LoD.BASIC, TAG, "Operator Test succeded!", 
					"Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
					"Asserted Results: "+ "TODO.");
		}
		catch (AssertionError e){
			DebugLogger.printAndLog_Error(TAG, "Operator Test failed. For more infos, check the JUnit Failure Trace.", e.toString());
			failureTrace = e;
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		
		DebugLogger.printAndLog_Message(LoD.BASIC, TAG, "Summary:",
										"Number of Input-Values: "+ inputTuples.size(),
										"Number of Ouput-Values: "+ outputVals.size(),
										"Time to execute input:"+ inputTimeDiff +" ms. \n");
		
		
		DebugLogger.printAndLog_Message(LoD.BASIC, TAG, DebugLogger.print_Header("Finished Operator Test!.", '='));
		
		if(failureTrace != null)
			throw failureTrace;
	}
	
	
	@After
	public void exitTestSetup()
	{
		inputTuples = null;
		operator = null;
	}
	
	
	
	abstract protected void configureDebugLogger();
	abstract protected List<Tuple> generateInputValues();
	abstract protected IOperator initOperator(final List<Tuple> inputTuples);
	
	abstract protected List<Object> assertOutput(final List<Tuple> inputTuples);
}
