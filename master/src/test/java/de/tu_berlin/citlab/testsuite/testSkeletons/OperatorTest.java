package de.tu_berlin.citlab.testsuite.testSkeletons;


import java.util.List;

import backtype.storm.tuple.Fields;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


abstract public class OperatorTest implements OperatorTestMethods
{

/* Global Private Constants: */
/* ========================= */

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.OPTEST_ID);
    private static final Logger HEADLINER = LogManager.getLogger(DebugLogger.HEADER_ID);
    private static final Marker BASIC = DebugLogger.getBasicMarker();
    private static final Marker DEFAULT = DebugLogger.getDefaultMarker();


/* Global Variables: */
/* ================= */

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
//		DebugLogger.setFileLogging(testName + "/operator", "TupleMock.log", LoD.DETAILED, TupleMock.TAG);
//		DebugLogger.setFileLogging(testName + "/operator", "UDFBoltMock.log", LoD.DETAILED, UDFBoltMock.TAG);
//        DebugLogger.setFileLogging(testName + "/operator", "OutputCollectorMock.log", LoD.DETAILED, OutputCollectorMock.TAG);
//        DebugLogger.setFileLogging(testName, "OperatorTest.log", LoD.DETAILED, logTag);
		
		LOGGER.debug(DebugPrinter.print_Header("Initializing Operator-Test Setup [" + testName + "]...", '-'));

		try{
			inputTuples = this.generateInputTuples();
            LOGGER.debug(DEFAULT, "Input-Tuples are: {}", DebugPrinter.toTupleListString(inputTuples));
		}
		catch (NullPointerException e){
            String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
            LOGGER.error(errorMsg, e);
			throw new NullPointerException(errorMsg);
			
		}			
		try{
			operator = this.initOperator(inputTuples);
            LOGGER.debug(DEFAULT, "Operator successfully initialized.");
		}
		catch (NullPointerException e){
			String errorMsg = "Operator must not be null! Return it in initOperator(..)";
            LOGGER.error(errorMsg, e);
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

        HEADLINER.debug(BASIC, DebugPrinter.print_Header("Starting Operator Test [" + testName + "]...", '='));


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

            LOGGER.debug(BASIC, "Operator Test succeded! \n\t Output Results: {} \n\t Asserted Results: {}",
                    DebugPrinter.toObjectWindowString(outputVals),
                    DebugPrinter.toObjectWindowString(assertRes));

        }
        catch (AssertionError e){
            LOGGER.error("Operator Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: {} \n\t Asserted Results: {}",
                    DebugPrinter.toObjectWindowString(outputVals),
                    DebugPrinter.toObjectWindowString(assertRes),
                    e);
            failureTrace = e;
        }


        LOGGER.debug(BASIC, "Summary [{}]: \n\t Number of Input-Tuples: {} \n\t Number of Output-Values: {} \n\t Time to execute input: {} ms.",
                    testName,
                    inputTuples.size(),
                    outputVals.size(),
                    inputTimeDiff);

        HEADLINER.debug(BASIC, DebugPrinter.print_Footer("Finished Operator Test!", '='));


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
