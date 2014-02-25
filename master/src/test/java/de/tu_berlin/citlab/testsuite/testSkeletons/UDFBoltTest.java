package de.tu_berlin.citlab.testsuite.testSkeletons;

import java.util.List;

import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.mocks.UDFBoltMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import static org.junit.Assert.assertTrue;


abstract public class UDFBoltTest implements UDFBoltTestMethods
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
	private IKeyConfig keyConfig;


    public final OperatorTest getOpTest() { return opTest; }
	

    public UDFBoltTest(String testName, OperatorTest opTest, Fields outputFields)
    {
        this.logTag = "BoltTest_"+testName;
        this.testName = testName;
        this.opTest = opTest;
        this.outputFields = outputFields;
    }



	private void initTestSetup()
	{
//        DebugLogger.setFileLogging(testName + "/bolt", "TupleMock.log", DebugLogger.LoD.DETAILED, TupleMock.TAG);
//        DebugLogger.setFileLogging(testName + "/bolt", "OutputCollectorMock.log", DebugLogger.LoD.DETAILED, OutputCollectorMock.TAG);
//        DebugLogger.setFileLogging(testName + "/bolt", "UDFBoltMock.log", DebugLogger.LoD.DETAILED, UDFBoltMock.TAG);
//        DebugLogger.setFileLogging(testName, "BoltTest.log", DebugLogger.LoD.DETAILED, logTag);

        LOGGER.debug(DEFAULT, DebugPrinter.printHeader("Initializing Bolt-Test Setup [" + testName + "]...", '-'));

        try{
            inputTuples = this.generateInputTuples();
            LOGGER.debug(DEFAULT, "Input-Tuples are: \n\t {}", DebugPrinter.toTupleListString(inputTuples));
        }
        catch (NullPointerException e){
            String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
            LOGGER.error(errorMsg, e);
            throw new NullPointerException(errorMsg);
        }

		IOperator operator = opTest.initOperator(inputTuples);
		window = this.initWindow();
		keyConfig = this.initKeyConfig();
		
		if (window == null){
			udfBolt = new UDFBoltMock(outputFields, operator);

            //Logging:
            LOGGER.debug(DEFAULT, "Initialized windowless UDFBolt.");
        }
		else if(keyConfig == null){
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
			udfBolt = new UDFBoltMock(outputFields, operator, window, keyConfig);

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
	
	
//	@Test
	public void testUDFBolt()
	{
        this.initTestSetup();

        AssertionError failureTrace = null;

        HEADLINER.debug(BASIC, DebugPrinter.printHeader("Starting Bolt Test [" + testName + "]...", '='));


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
        List<List<Object>> assertRes = this.assertOutput(inputTuples);


		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;


        try{
            assertTrue("UDFBolt.execute(..) result is not equal to asserted Output from OperatorTest! \n", assertRes.equals(outputVals));

            LOGGER.debug(BASIC, "Bolt Test succeded! \n\t Output Results: {} \n\t Asserted Results: {}",
                    DebugPrinter.toObjectWindowString(outputVals),
                    DebugPrinter.toObjectWindowString(assertRes));
        }
        catch (AssertionError e){
            LOGGER.error("Bolt Test failed. For more infos, check the JUnit Failure Trace. \n\t Output Results: {} \n\t Asserted Results: {}",
                    DebugPrinter.toObjectWindowString(outputVals),
                    DebugPrinter.toObjectWindowString(assertRes),
                    e);
            failureTrace = e;
        }


        LOGGER.debug(BASIC, "Summary [{}]: \n\t Number of Input-Tuples: {} \n\t Number of Tick-Tuples: {} \n\t Number of Output-Values: {} \n\t Time to execute input: {} ms.",
                    testName,
                    inputTuples.size(),
                    tickTupleCount,
                    outputVals.size(),
                    inputTimeDiff);

        HEADLINER.debug(BASIC, DebugPrinter.printFooter("Finished Bolt Test!", '='));


        if(failureTrace != null)
            throw failureTrace;
	}
	
	
//	@After
	public void terminateTestSetup()
	{
		udfBolt = null;
		
//		udfFields = null;
//		operator = null;
		window = null;
		keyConfig = null;
	}
	

//
	public List<Tuple> generateInputTuples()
    {
        return opTest.generateInputTuples();
    }

	public List<List<Object>> assertOutput(List<Tuple> inputTuples)
    {
        return opTest.assertOutput(inputTuples);
    }


    abstract public Window<Tuple, List<Tuple>> initWindow();

    abstract public IKeyConfig initKeyConfig();

}
