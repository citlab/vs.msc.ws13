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
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;

import static org.junit.Assert.assertTrue;


abstract public class OperatorTest implements OperatorTestMethods
{
	public final static String TAG = "OperatorTest";

    private final String testName;
    private final Fields inputFields;

	private List<Tuple> inputTuples;
	private IOperator operator;

    public final Fields getInputFields() { return inputFields; }



    public OperatorTest(String testName, Fields inputFields)
    {
        this.testName = testName;
        this.inputFields = inputFields;

        this.initTestSetup();
    }

//	@Before
	public void initTestSetup()
	{
		//Will be overridden from configureDebugLogger() if set there explicitly:
		DebugLogger.addFileLogging(testName+"/operator", "TupleMock.log", LoD.DETAILED, TupleMock.TAG);
		DebugLogger.addFileLogging(testName+"/operator", "OutputCollectorMock.log", LoD.DETAILED, OutputCollectorMock.TAG);
		DebugLogger.addFileLogging(testName, "OperatorTest.log", LoD.DETAILED, TAG);
//		this.configureDebugLogger();
		
		
		String header = DebugLogger.print_Header("Initializing Operator-Test Setup...", '-');
		DebugLogger.log_Message(LoD.DEFAULT, TAG, header);
		
		try{
			inputTuples = this.generateInputTuples();
			DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, "Input-Tuples are: ", DebugPrinter.toTupleListString(inputTuples));
		}
		catch (NullPointerException e){
			String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
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
	

//	@Test
	public void testOperator()
	{
		AssertionError failureTrace = null;
		
		String header = DebugLogger.print_Header(LoD.BASIC, "Starting Operator Test...", '=');
		DebugLogger.log_Message(LoD.BASIC, TAG, header);


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		operator.execute(inputTuples, outputCollector);


		List<List<Object>> outputVals = OutputCollectorMock.output;
		List<List<Object>> assertRes = assertOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            assertTrue("Operator.execute(..) result is not equal to asserted Output! \n", assertRes.equals(outputVals));

            DebugLogger.printAndLog_Message(LoD.BASIC, TAG, "Operator Test succeded!",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes));
        }
        catch (AssertionError e){
            DebugLogger.printAndLog_Error(TAG, "Operator Test failed. For more infos, check the JUnit Failure Trace.",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes),
                    e.toString());
            failureTrace = e;
        }
		
		
		DebugLogger.printAndLog_Message(LoD.BASIC, TAG, "Summary:",
										"Number of Input-Tuples: "+ inputTuples.size(),
										"Number of Output-Values: "+ outputVals.size(),
										"Time to execute input:"+ inputTimeDiff +" ms.");
		
		
		String footer = DebugLogger.print_Footer("Finished Operator Test!", '=');
		DebugLogger.log_Message(LoD.BASIC, TAG, footer);
		
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
