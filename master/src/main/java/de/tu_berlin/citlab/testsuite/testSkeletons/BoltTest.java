package de.tu_berlin.citlab.testsuite.testSkeletons;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.charts.BoltTupleChart;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.LogPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.mocks.UDFBoltMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.junit.Assert;

import java.util.List;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


/**
 * <p>
 *     The BoltTest is an <em><b>abstract Test-Skeleton</b></em> which is used to test the
 *     {@link de.tu_berlin.citlab.storm.udf.IOperator IOperator} in it's natural environment:
 *     Inside a {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}.<br />
 *     This includes the {@link de.tu_berlin.citlab.storm.window.Window Window} and
 *     {@link de.tu_berlin.citlab.storm.window.WindowHandler WindowHandler} that is used by the Bolt's definition
 *     on a pre-defined {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock inputTuples}
 *     and the respective asserted Output.
 * </p>
 * <p>
 *     This BoltTest also includes the {@link de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest OperatorTest} that
 *     should be tested in an extra test by itself. If both tests, the <b>OperatorTest</b> as well as the <b>BoltTest</b>
 *     are running completely, one can assert that the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt},
 *     independent from its topology, is doing what it should.
 * </p>
 * <p>
 *     A successful test is defined via the {@link BoltTest#assertWindowedOutput(java.util.List)} definition in
 *     the BoltTest's implementation. If this method returns <b>null</b> as the asserted-Output, the assertion mechanism
 *     will be deactivated and the test will always succeed (of interest for {@link TopologyTest TopologyTests}).
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
abstract public class BoltTest implements UDFBoltTestMethods
{

/* Global Private Constants: */
/* ========================= */

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.BOLTTEST_ID);
    private static final Logger HEADLINER = LogManager.getLogger(DebugLogger.HEADER_ID);
    private static final Marker BASIC = DebugLogger.getBasicMarker();
    private static final Marker DEFAULT = DebugLogger.getDefaultMarker();


/* Global Variables: */
/* ================= */

//    public final String logDir;
    public final String testName;

    private final OperatorTest opTest;
    private final Fields outputFields;

    private final BoltTupleChart tupleChart;

	private UDFBoltMock udfBolt;
	
	private List<Tuple> inputTuples;

	private Window<Tuple, List<Tuple>> window;
	private WindowHandler windowHandler;



/* The Constructor: */
/* ================ */

    /**
     * Constructor, used to build BoltTests.
     * <p>
     *     BoltTests are used to test the {@link de.tu_berlin.citlab.storm.udf.IOperator IOperator}
	 *     in it's natural environment: Inside a {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}.
	 *     This includes the {@link de.tu_berlin.citlab.storm.window.Window Window} and {@link de.tu_berlin.citlab.storm.window.WindowHandler WindowHandler}
	 *     that is used by the Bolt's definition.
     * </p>
     * @param testName The name of this Bolt-Test (later used for identification in LogFiles)
     * @param opTest The corresponding {@link OperatorTest}, being used to test the Operator, linked to that UDFBolt.
     * @param outputFields The output-{@link backtype.storm.tuple.Fields}, used by the UDFBolt for it's output-emission.
     */
    public BoltTest(String testName, OperatorTest opTest, Fields outputFields)
    {
//        this.logDir = "BoltTest_"+testName;
        this.testName = testName;
        System.setProperty("logTestName", testName);
        org.apache.logging.log4j.core.LoggerContext ctx =
                (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();

        tupleChart = new BoltTupleChart(testName);

        this.opTest = opTest;
        this.outputFields = outputFields;
    }



/* Public Methods for Test-Setup: */
/* ============================== */

	/**
	 * The {@link BoltTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em><b>test initialization</b></em>, the <em>test-run</em> itself
	 * and the <em>test-termination</em>.
	 * <p>
	 *     The initTestSetup method is initializing the input-{@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMock}
	 *     as a parameter, and via the abstract methods {@link BoltTest#initWindowHandler()} & {@link BoltTest#initWindow()}
	 *     the regarding {@link de.tu_berlin.citlab.storm.window.WindowHandler} & {@link de.tu_berlin.citlab.storm.window.Window},
	 *	   belonging to the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} for that test.
	 * </p>
	 * @param inputTuples The inputTuples as a {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMocks}
	 */
	public void initTestSetup(List<Tuple> inputTuples)
	{
        LOGGER.debug(DEFAULT, LogPrinter.printHeader("Initializing Bolt-Test Setup [" + testName + "]...", '-'));
		LOGGER.debug(DEFAULT, "Output-Fields are: \n {}", LogPrinter.toFieldsString(outputFields));

        try{
            this.inputTuples = inputTuples;
            LOGGER.debug(DEFAULT, "Input-Tuples are: \n {}", LogPrinter.toTupleListString(inputTuples));
        }
        catch (NullPointerException e){
            String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
            LOGGER.error(BASIC, errorMsg, e);
            throw new NullPointerException(errorMsg);
        }

		IOperator operator = opTest.initOperator();
		window = this.initWindow();
		windowHandler = this.initWindowHandler();
		
		if (window == null){
			udfBolt = new UDFBoltMock(outputFields, operator);

            //Logging:
            LOGGER.debug(DEFAULT, "Initialized windowless UDFBolt.");
        }
		else if(windowHandler == null){
			udfBolt = spy(new UDFBoltMock(outputFields, operator, window));

            //Logging:
            if(window.getClass().equals(CountWindow.class))
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with Count-Window.");
            else if(window.getClass().equals(TimeWindow.class))
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with Time-Window.");
            else
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with unknown Window type.");
        }
		else{
			udfBolt = new UDFBoltMock(outputFields, operator, window, windowHandler.getWindowKey(), windowHandler.getGroupByKey());

            //Logging:
            if(window.getClass().equals(CountWindow.class))
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with Count-Window & KeyConfig.");
            else if(window.getClass().equals(TimeWindow.class))
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with Time-Window & KeyConfig.");
            else
                LOGGER.debug(DEFAULT, "Initialized UDFBolt with unknown Window type & KeyConfig.");
        }


        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}


	/**
	 * The {@link BoltTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em>test initialization</em>, the <em><b>test-run</b></em> itself
	 * and the <em>test-termination</em>. <br />
	 * <em>Being a part of the JUnit lifecycle, this method is used in a @Test method.</em>
	 * <p>
	 *	   This test-method is testing the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}, linked to this BoltTest.
	 *	   It executes the inputTuples, previously set by the {@link BoltTest#initTestSetup(java.util.List)} by the
	 *	   {@link de.tu_berlin.citlab.storm.bolts.UDFBolt#execute(backtype.storm.tuple.Tuple) UDFBolt.execute(Tuple)} method.
	 * </p>
	 * <p>
	 *     Additionally, the parameter <em>sleepTimeBetweenTuples</em> sets the sleepTime in Milliseconds for
	 *     {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow}-Tests. This should be set to zero for any other
	 *     tests (windowless or based on a {@link de.tu_berlin.citlab.storm.window.CountWindow CountWindow}.
	 * </p>
	 * @param sleepTimeBetweenTuples The {@link Thread#sleep(long) Thread.sleep(int milliseconds)} time between each tuple execution.
	 *                               This should be set to zero if no {@link de.tu_berlin.citlab.storm.window.CountWindow CountWindow} is used.
	 * @return The {@link de.tu_berlin.citlab.testsuite.helpers.BoltEmission BoltEmission} of the {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock}.
	 */
	public BoltEmission testUDFBolt(int sleepTimeBetweenTuples)
	{
        AssertionError failureTrace = null;

        HEADLINER.debug(BASIC, LogPrinter.printHeader("Starting Bolt Test [" + testName + "]...", '='));


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


        int tickTupleCount = 0;
		for(Tuple actTuple : inputTuples){
            if(TupleHelper.isTickTuple(actTuple))
                tickTupleCount ++;

			if(sleepTimeBetweenTuples > 0){
				try {
					Thread.sleep(sleepTimeBetweenTuples);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//Execution of the main Handler in the UDF-Bolt:
			udfBolt.execute(actTuple);
            tupleChart.addTupleToChart(actTuple, System.currentTimeMillis());

            if(udfBolt.wasJustFlushed()){
                tupleChart.flushWindow();
            }
		}

        List<List<Object>> outputVals = OutputCollectorMock.output;
        List<List<Object>> assertRes = this.assertWindowedOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;

        tupleChart.createChart(startTime, endTime);


        try{
			if(assertRes != null){
            	Assert.assertTrue("UDFBolt.execute(..) result is not equal to asserted Output from OperatorTest! \n", assertRes.equals(outputVals));
				LOGGER.debug(BASIC, "Bolt Test succeded! \n\t Output Results: \n {} \n\t Asserted Results: \n {}",
						LogPrinter.toObjectWindowString(outputVals),
						LogPrinter.toObjectWindowString(assertRes));
			}
			else { //If assertRes is null, outputAssertion is deactivated. TestSuite is used for local logging only then.
				LOGGER.info(BASIC, "Assertion is deactivated, as asserted-Results are not set by user and thus null.");
				LOGGER.debug(BASIC, "Bolt Output: \n {}", LogPrinter.toObjectWindowString(outputVals));
			}
        }
        catch (AssertionError e){
            LOGGER.error(BASIC, "Bolt Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: \n {} \n\t Asserted Results: \n {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes));
            failureTrace = e;
        }


        LOGGER.debug(BASIC, "Summary [{}]: \n\t Number of Input-Tuples: {} \n\t Number of Tick-Tuples: {} \n\t Number of Output-Values: {} \n\t Time to execute input: {} ms.",
                    testName,
                    inputTuples.size(),
                    tickTupleCount,
                    outputVals.size(),
                    inputTimeDiff);

        HEADLINER.debug(BASIC, LogPrinter.printFooter("Finished Bolt Test!", '='));


        if(failureTrace != null)
            throw failureTrace;

        BoltEmission boltEmission = new BoltEmission(outputVals, outputFields);
        return boltEmission;
	}
	

	/**
	 * The {@link BoltTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through the
	 * complete UnitTest-lifecycle. This includes a <em>test initialization</em>, the <em>test-run</em> itself
	 * and the <em><b>test-termination</b></em>.
	 * <p>
	 *     This method terminates (sets to <b>null</b>) every object from the
	 *     {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} that was set up for the test.
	 * </p>
	 */
	public void terminateTestSetup()
	{
		opTest.terminateTestSetup();
		udfBolt = null;
		window = null;
		windowHandler = null;
	}
}
