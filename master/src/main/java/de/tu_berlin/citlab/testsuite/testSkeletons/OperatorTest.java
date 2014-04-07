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


/**
 * <p>
 *     The OperatorTest is an <em><b>abstract Test-Skeleton</b></em> whose implementation class is representing a test case
 *     with a given {@link de.tu_berlin.citlab.storm.udf.IOperator IOperator}, the pre-defined {@link java.util.List}
 *     of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock inputTuples} and the respective asserted Output.
 * </p>
 * <p>
 *     A successful test is defined via the {@link OperatorTest#assertOperatorOutput(java.util.List)} definition
 *     in the OperatorTests's implementation: If the asserted Output is equal to the real output from the OutputCollector's
 *     emission, the test will succeed. <br />
 *     On the other hand, if this method returns <b>null</b> as the asserted-Output, the assertion
 *     mechanism will be deactivated and the test will always succeed (of interest for {@link TopologyTest TopologyTests}).
 * </p>
 * <p>
 *     For a combinatorial test of both, a <b>BoltTest</b> and an <b>OperatorTest</b>, implement the
 *     {@link de.tu_berlin.citlab.testsuite.testSkeletons.StandaloneTest StandaloneTest} test-skeleton.<br />
 *     If a complete topology should be tested, take the
 *     {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest} test-skeleton
 *     as your choice.
 * </p>
 * @author Constantin
 */
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

    public final String testName;
//    public final String logDir;

    private List<Tuple> inputTuples;

    private IOperator operator;



/* The Constructor: */
/* ================ */

    public OperatorTest(String testName)
    {
        this.testName = testName;
//        this.logDir = "OperatorTest_"+testName;

        System.setProperty("logTestName", testName);

        org.apache.logging.log4j.core.LoggerContext ctx =
                (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();
    }



/* Public Methods for Test-Setup: */
/* ============================== */

	/**
	 * The {@link OperatorTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em><b>test initialization</b></em>, the <em>test-run</em> itself
	 * and the <em>test-termination</em>.
	 * <p>
	 *     The initTestSetup method is initializing the {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock inputTuples}
	 *     as a parameter, and via the abstract method {@link OperatorTest#initOperator()}
	 *     the regarding {@link de.tu_berlin.citlab.storm.udf.IOperator}, representing the
	 *     {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}'s user-defined-function for that test.
	 * </p>
	 * @param inputTuples The inputTuples as a {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMocks}
	 */
	public void initTestSetup(List<Tuple> inputTuples)
	{
		LOGGER.debug(LogPrinter.printHeader("Initializing Operator-Test Setup [" + testName + "]...", '-'));
        this.inputTuples = inputTuples;

		try{
			operator = this.initOperator();
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


	/**
	 * The {@link OperatorTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em>test initialization</em>, the <em><b>test-run</b></em> itself
	 * and the <em>test-termination</em>. <br />
	 * <em>Being a part of the JUnit lifecycle, this method is used in a @Test method.</em>
	 * <p>
	 *	   This test-method is testing the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}'s
	 *	   {@link de.tu_berlin.citlab.storm.udf.IOperator}, linked to this BoltTest. <br />
	 *	   It executes the inputTuples, previously set by the {@link BoltTest#initTestSetup(java.util.List)} by the
	 *	   {@link de.tu_berlin.citlab.storm.udf.IOperator#execute(java.util.List, backtype.storm.task.OutputCollector)} method.
	 * </p>
	 */
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


	/**
	 * The {@link OperatorTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em>test initialization</em>, the <em>test-run</em> itself
	 * and the <em><b>test-termination</b></em>.
	 * <p>
	 *     This method terminates (sets to <b>null</b>) every object from the
	 *     {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} that was set up for the test.
	 * </p>
	 */
	public void terminateTestSetup()
	{
		operator = null;
	}
}
