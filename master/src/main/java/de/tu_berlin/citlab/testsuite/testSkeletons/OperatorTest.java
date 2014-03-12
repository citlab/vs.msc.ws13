package de.tu_berlin.citlab.testsuite.testSkeletons;


import java.util.List;

import backtype.storm.tuple.Fields;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.LogPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.junit.Assert;

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
//    private final Fields inputFields;
    private List<Tuple> inputTuples;

    private IOperator operator;

//    public final Fields getInputFields() { return inputFields; }


    public OperatorTest(String testName)
    {
        this.logTag = "OperatorTest_"+testName;
        this.testName = testName;
//        this.inputFields = inputFields;
//        this.inputTuples = inputTuples;
    }



	public void initTestSetup(List<Tuple> inputTuples)
	{
		LOGGER.debug(LogPrinter.printHeader("Initializing Operator-Test Setup [" + testName + "]...", '-'));


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



	public void testOperator()
	{
//        this.initTestSetup(); TODO: add initTestSetup in tests.

		AssertionError failureTrace = null;

        HEADLINER.debug(BASIC, LogPrinter.printHeader("Starting Operator Test [" + testName + "]...", '='));


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		try {
			operator.execute(inputTuples, outputCollector);
		} catch (OperatorException e) {
			LOGGER.error("Operator execution failed!", e);
		}


		List<List<Object>> outputVals = OutputCollectorMock.output;
		List<List<Object>> assertRes = assertOperatorOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            if(assertRes != null)
                Assert.assertTrue("Operator.execute(..) result is not equal to asserted Output! \n", assertRes.equals(outputVals));
            else //If assertRes is null, the output vals also needs to be null, so that assertRes == outputVals:
                Assert.assertNull(outputVals);

            LOGGER.debug(BASIC, "Operator Test succeded! \n\t Output Results: {} \n\t Asserted Results: {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes));

        }
        catch (AssertionError e){
            LOGGER.error("Operator Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: {} \n\t Asserted Results: {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes),
                    e);
            failureTrace = e;
        }


        LOGGER.debug(BASIC, "Summary [{}]: \n\t Number of Input-Tuples: {} \n\t Number of Output-Values: {} \n\t Time to execute input: {} ms.",
                    testName,
                    inputTuples.size(),
                    outputVals.size(),
                    inputTimeDiff);

        HEADLINER.debug(BASIC, LogPrinter.printFooter("Finished Operator Test!", '='));


		if(failureTrace != null)
			throw failureTrace;
	}
	

	public void terminateTestSetup()
	{
		operator = null;
	}


	abstract public IOperator initOperator(final List<Tuple> inputTuples);
	abstract public List<List<Object>> assertOperatorOutput(final List<Tuple> inputTuples);
}
