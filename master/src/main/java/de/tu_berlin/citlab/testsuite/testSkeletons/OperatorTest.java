package de.tu_berlin.citlab.testsuite.testSkeletons;


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

import java.util.List;


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
    private List<Tuple> inputTuples;

    private IOperator operator;



/* The Constructor: */
/* ================ */

    public OperatorTest(String testName)
    {
        this.logTag = "OperatorTest_"+testName;
        this.testName = testName;
    }



/* Public Methods for Test-Setup: */
/* ============================== */

	public void initTestSetup(List<Tuple> inputTuples)
	{
		LOGGER.debug(LogPrinter.printHeader("Initializing Operator-Test Setup [" + testName + "]...", '-'));
        this.inputTuples = inputTuples;

		try{
			operator = this.initOperator(inputTuples);
            LOGGER.debug(DEFAULT, "Operator successfully initialized.");
		}
		catch (NullPointerException e){
			String errorMsg = "Operator must not be null! Return it in initOperator(..)";
            LOGGER.error(BASIC, errorMsg, e);
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
		AssertionError failureTrace = null;

        HEADLINER.debug(BASIC, LogPrinter.printHeader("Starting Operator Test [" + testName + "]...", '='));


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


		OutputCollector outputCollector = OutputCollectorMock.mockOutputCollector();
		try {
			operator.execute(inputTuples, outputCollector);
		} catch (OperatorException e) {
			LOGGER.error(BASIC, "Operator execution failed!", e);
		}


		List<List<Object>> outputVals = OutputCollectorMock.output;
		List<List<Object>> assertRes = assertOperatorOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            if(assertRes != null){
				Assert.assertTrue("Operator.execute(..) result is not equal to asserted Output! \n", assertRes.equals(outputVals));
				LOGGER.debug(BASIC, "Operator Test succeded! \n\t Output Results: \n {} \n\t Asserted Results: \n {}",
						LogPrinter.toObjectWindowString(outputVals),
						LogPrinter.toObjectWindowString(assertRes));
			}
            else { //If assertRes is null, outputAssertion is deactivated. TestSuite is used for local logging only then.
            	LOGGER.info(BASIC, "Assertion is deactivated, as asserted-Results are not set by user and thus null.");
				LOGGER.debug(BASIC, "Operator Output: \n {}", LogPrinter.toObjectWindowString(outputVals));
			}
        }
        catch (AssertionError e){
            LOGGER.error(BASIC, "Operator Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: \n {} \n\t Asserted Results: \n {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes));
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



/* Abstract OperatorTestMethods Interfaces : */
/* ======================================== */

	abstract public IOperator initOperator(final List<Tuple> inputTuples);
	abstract public List<List<Object>> assertOperatorOutput(final List<Tuple> inputTuples);
}
