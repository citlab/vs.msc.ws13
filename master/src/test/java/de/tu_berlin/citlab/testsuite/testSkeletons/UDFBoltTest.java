package de.tu_berlin.citlab.testsuite.testSkeletons;

import java.util.List;

import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.mocks.UDFBoltMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;

import static org.junit.Assert.assertTrue;


abstract public class UDFBoltTest implements UDFBoltTestMethods
{
    public final static String TAG = "BoltTest";

    private final String testName;
    private final OperatorTest opTest;
    private final Fields outputFields;
	private UDFBoltMock udfBolt;
	
	private List<Tuple> inputTuples;
    public final List<Tuple> getOperatorInputTuples() { return opTest.generateInputTuples(); }
//	private UDFFields udfFields;

//	private IOperator operator;
	private Window<Tuple, List<Tuple>> window;
	private IKeyConfig keyConfig;
	

    public UDFBoltTest(String testName, OperatorTest opTest, Fields outputFields)
    {
        this.testName = testName;
        this.opTest = opTest;
        this.outputFields = outputFields;

        this.initTestSetup();
    }

//	@Before
	public void initTestSetup()
	{
        DebugLogger.addFileLogging(testName+"/bolt", "TupleMock.log", DebugLogger.LoD.DETAILED, TupleMock.TAG);
        DebugLogger.addFileLogging(testName+"/bolt", "OutputCollectorMock.log", DebugLogger.LoD.DETAILED, OutputCollectorMock.TAG);
        DebugLogger.addFileLogging(testName, "BoltTest.log", DebugLogger.LoD.DETAILED, TAG);

        String header = DebugLogger.print_Header("Initializing Bolt-Test Setup...", '-');
        DebugLogger.log_Message(DebugLogger.LoD.DEFAULT, TAG, header);

        try{
            inputTuples = this.generateInputTuples();
            DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Input-Tuples are: ", DebugPrinter.toTupleListString(inputTuples));
        }
        catch (NullPointerException e){
            String errorMsg = "InputTuples must not be null! \n Return them in generateInputTuples().";
            DebugLogger.printAndLog_Error(TAG, errorMsg, e.toString());
            throw new NullPointerException(errorMsg);
        }

		IOperator operator = opTest.initOperator(inputTuples);
		window = this.initWindow();
		keyConfig = this.initKeyConfig();
		
		if (window == null){
			udfBolt = new UDFBoltMock(outputFields, operator);

            //Logging:
            DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized windowless UDFBolt.");
        }
		else if(keyConfig == null){
			udfBolt = new UDFBoltMock(outputFields, operator, window);

            //Logging:
            if(window.getClass().equals(CountWindow.class))
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with Count-Window.");
            else if(window.getClass().equals(TimeWindow.class))
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with Time-Window.");
            else
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with unknown Window type.");
        }
		else{
			udfBolt = new UDFBoltMock(outputFields, operator, window, keyConfig);

            //Logging:
            if(window.getClass().equals(CountWindow.class))
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with Count-Window & KeyConfig.");
            else if(window.getClass().equals(TimeWindow.class))
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with Time-Window & KeyConfig.");
            else
                DebugLogger.printAndLog_Message(DebugLogger.LoD.DEFAULT, TAG, "Initialized UDFBolt with unknown Window type & KeyConfig.");
        }
	}
	
	
//	@Test
	public void testUDFBolt()
	{
        AssertionError failureTrace = null;

        String header = DebugLogger.print_Header(DebugLogger.LoD.BASIC, "Starting Bolt Test...", '=');
        DebugLogger.log_Message(DebugLogger.LoD.BASIC, TAG, header);


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

            DebugLogger.printAndLog_Message(DebugLogger.LoD.BASIC, TAG, "Bolt Test succeded!",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes));
        }
        catch (AssertionError e){
            DebugLogger.printAndLog_Error(TAG, "Bolt Test failed. For more infos, check the JUnit Failure Trace.",
                    "Output Results: "+ DebugPrinter.toObjectWindowString(outputVals),
                    "Asserted Results: "+ DebugPrinter.toObjectWindowString(assertRes),
                    e.toString());
            failureTrace = e;
        }


        DebugLogger.printAndLog_Message(DebugLogger.LoD.BASIC, TAG, "Summary:",
                "Number of Input-Tuples: "+ inputTuples.size(),
                "Number of Tick-Tuples: "+ tickTupleCount,
                "Number of Output-Values: "+ outputVals.size(),
                "Time to execute input:"+ inputTimeDiff +" ms.");


        String footer = DebugLogger.print_Footer("Finished Bolt Test!", '=');
        DebugLogger.log_Message(DebugLogger.LoD.BASIC, TAG, footer);

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
