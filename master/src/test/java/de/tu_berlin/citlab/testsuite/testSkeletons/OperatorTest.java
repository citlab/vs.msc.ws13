package de.tu_berlin.citlab.testsuite.testSkeletons;


import java.util.List;

import backtype.storm.tuple.Fields;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.mocks.UDFBoltMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


abstract public class OperatorTest implements OperatorTestMethods
{
	public final String logTag;

    private final String testName;
    private final Fields inputFields;

	private List<Tuple> inputTuples;
	private IOperator operator;

    public final Fields getInputFields() { return inputFields; }



    public OperatorTest(String testName, Fields inputFields)
    {
        this.logTag = "OperatorTest_"+testName;
        this.testName = testName;
        this.inputFields = inputFields;
    }



	private void initTestSetup()
	{
		DebugLogger.setFileLogging(testName + "/operator", "TupleMock.log", LoD.DETAILED, TupleMock.TAG);
		DebugLogger.setFileLogging(testName + "/operator", "UDFBoltMock.log", LoD.DETAILED, UDFBoltMock.TAG);
        DebugLogger.setFileLogging(testName + "/operator", "OutputCollectorMock.log", LoD.DETAILED, OutputCollectorMock.TAG);
        DebugLogger.setFileLogging(testName, "OperatorTest.log", LoD.DETAILED, logTag);
		
		
		String header = DebugLogger.print_Header("Initializing Operator-Test Setup ["+testName +"]...", '-');
		DebugLogger.log_Message(LoD.DEFAULT, logTag, header);
		
		try{
			inputTuples = this.generateInputTuples();
			DebugLogger.printAndLog_Message(LoD.DEFAULT, logTag, "Input-Tuples are: ", DebugPrinter.toTupleListString(inputTuples));
		}
		catch (NullPointerException e){
			String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
			DebugLogger.printAndLog_Error(logTag, errorMsg, e.toString());
			throw new NullPointerException(errorMsg);
			
		}			
		try{
			operator = this.initOperator(inputTuples);
			DebugLogger.printAndLog_Message(LoD.DEFAULT, logTag, "Operator successfully initialized.");
		}
		catch (NullPointerException e){
			String errorMsg = "Operator must not be null! Return it in initOperator(..)";
			DebugLogger.printAndLog_Error(logTag, errorMsg, e.toString());
			throw new NullPointerException(errorMsg);
		}


        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
	

//	@Test
	public void testOperator()
	{
        this.initTestSetup();

		AssertionError failureTrace = null;
		
		String header = DebugLogger.print_Header(LoD.BASIC, "Starting Operator Test ["+testName +"]...", '=');
		DebugLogger.log_Message(LoD.BASIC, logTag, header);


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		operator.execute(inputTuples, outputCollector);


		List<List<Object>> outputVals = OutputCollectorMock.output;
		List<List<Object>> assertRes = assertOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            if(assertRes != null)
                assertTrue("Operator.execute(..) result is not equal to asserted Output! \n", assertRes.equals(outputVals));
            else //If assertRes is null, the output vals also needs to be null, so that assertRes == outputVals:
                assertNull(outputVals);

            DebugLogger.printAndLog_Message(LoD.BASIC, logTag, "Operator Test succeded!",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes));
        }
        catch (AssertionError e){
            DebugLogger.printAndLog_Error(logTag, "Operator Test failed. For more infos, check the JUnit Failure Trace.",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes),
                    e.toString());
            failureTrace = e;
        }
		
		
		DebugLogger.printAndLog_Message(LoD.BASIC, logTag, "Summary ["+testName +"]:",
										"Number of Input-Tuples: "+ inputTuples.size(),
										"Number of Output-Values: "+ outputVals.size(),
										"Time to execute input:"+ inputTimeDiff +" ms.");
		
		
		String footer = DebugLogger.print_Footer("Finished Operator Test!", '=');
		DebugLogger.log_Message(LoD.BASIC, logTag, footer);
		
		if(failureTrace != null)
			throw failureTrace;
	}
	
	
//	@After
	public void terminateTestSetup()
	{
		inputTuples = null;
		operator = null;
	}
	
	

	abstract public List<Tuple> generateInputTuples();

	abstract public IOperator initOperator(final List<Tuple> inputTuples);
	
	abstract public List<List<Object>> assertOutput(final List<Tuple> inputTuples);
}
