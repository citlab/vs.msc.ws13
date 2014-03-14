package de.tu_berlin.citlab.testsuite.testSkeletons;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
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

    public final String logTag;

    private final String testName;
    private final OperatorTest opTest;
    private final Fields outputFields;
	private UDFBoltMock udfBolt;
	
	private List<Tuple> inputTuples;

	private Window<Tuple, List<Tuple>> window;
	private WindowHandler windowHandler;


//    public final OperatorTest getOpTest() { return opTest; }


    /**
     * Constructor, used to build up a Topology in a {@link TopologyTest}.
     * <p>
     *     This Topology-Constructor is taking into account, that a predecessing <b>UDFBolt</b>
     *     (or more precisely it's predecessing {@link BoltTest}) has some outputValues,
     *     stored in a {@link de.tu_berlin.citlab.testsuite.helpers.BoltEmission}.
     * </p>
     * <p>
     *     <em><b>Notice:</b> If this Constructor is chosen, {@link BoltTest#generateInputTuples()}
     *     is not of relevance, as the inputTuples are the outputTuples of the predecessing bolt.</em>
     * </p>
     * @param testName The name of this Bolt-Test (later used for identification in LogFiles)
     * @param opTest The corresponding {@link OperatorTest}, being used to test the Operator, linked to that UDFBolt.
     * @param predecessorOutput The output of the predecessing {@link BoltTest#testUDFBolt()}-method.
     */
//    public BoltTest(String testName, OperatorTest opTest, BoltEmission predecessorOutput)
//    {
//        this(testName, opTest, predecessorOutput.outputFields);
//        this.inputTuples = predecessorOutput.tupleList;
//    }

    /**
     * Constructor, used to build standalone-tests.
     * <p>
     *     Standalone-tests are used outside of any topology and are thus not dependent
     *     on predecessor- or successor-bolts. As of that, only the Operator is tested by
     *     a specified input (defined by a test-developer in {@link BoltTest#initTestSetup(java.util.List)})
     *     and is compared against an assertedOutput.
     * </p>
     * @param testName The name of this Bolt-Test (later used for identification in LogFiles)
     * @param opTest The corresponding {@link OperatorTest}, being used to test the Operator, linked to that UDFBolt.
     * @param outputFields The output-{@link backtype.storm.tuple.Fields}, used by the UDFBolt for it's output-emission.
     */
    public BoltTest(String testName, OperatorTest opTest, Fields outputFields)
    {
        this.logTag = "BoltTest_"+testName;
        this.testName = testName;
        this.opTest = opTest;
        this.outputFields = outputFields;
    }



	public void initTestSetup(List<Tuple> inputTuples)//(List<Tuple> inputTuples, Fields outputFields)
	{
        LOGGER.debug(DEFAULT, LogPrinter.printHeader("Initializing Bolt-Test Setup [" + testName + "]...", '-'));

        try{
            //Only define inputTuples via the initTestSetup parameter, if they are not already
            //defined in the constructor:
            this.inputTuples = inputTuples;
//            this.outputFields = outputFields;
//            if(this.inputTuples == null){
//                this.inputTuples = inputTuples;
//            }
            LOGGER.debug(DEFAULT, "Input-Tuples are: \n\t {}", LogPrinter.toTupleListString(inputTuples));
        }
        catch (NullPointerException e){
            String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
            LOGGER.error(BASIC, errorMsg, e);
            throw new NullPointerException(errorMsg);
        }

		IOperator operator = opTest.initOperator(inputTuples);
		window = this.initWindow();
		windowHandler = this.initWindowHandler();
		
		if (window == null){
			udfBolt = new UDFBoltMock(outputFields, operator);

            //Logging:
            LOGGER.debug(DEFAULT, "Initialized windowless UDFBolt.");
        }
		else if(windowHandler == null){
			udfBolt = new UDFBoltMock(outputFields, operator, window);

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
	

	public BoltEmission testUDFBolt()
	{
        AssertionError failureTrace = null;

        HEADLINER.debug(BASIC, LogPrinter.printHeader("Starting Bolt Test [" + testName + "]...", '='));


        OutputCollectorMock.resetOutput();

		long startTime = System.currentTimeMillis();


        int tickTupleCount = 0;
		for(Tuple actTuple : inputTuples){
            if(TupleHelper.isTickTuple(actTuple))
                tickTupleCount ++;

            //Execution of the main Handler in the UDF-Bolt:
			udfBolt.execute(actTuple);
		}


        List<List<Object>> outputVals = OutputCollectorMock.output;
        List<List<Object>> assertRes = this.assertWindowedOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            Assert.assertTrue("UDFBolt.execute(..) result is not equal to asserted Output from OperatorTest! \n", assertRes.equals(outputVals));

            LOGGER.debug(BASIC, "Bolt Test succeded! \n\t Output Results: {} \n\t Asserted Results: {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes));
        }
        catch (AssertionError e){
            LOGGER.error(BASIC, "Bolt Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: {} \n\t Asserted Results: {}",
                    LogPrinter.toObjectWindowString(outputVals),
                    LogPrinter.toObjectWindowString(assertRes),
                    e);
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
	
	
//	@After
	public void terminateTestSetup()
	{
		udfBolt = null;

		window = null;
		windowHandler = null;
	}


	public List<List<Object>> assertWindowedOutput(List<Tuple> inputTuples)
    {
        return opTest.assertOperatorOutput(inputTuples);
    }


//    public abstract List<Tuple> generateInputTuples();

    public abstract Window<Tuple, List<Tuple>> initWindow();

    public abstract WindowHandler initWindowHandler();

}
