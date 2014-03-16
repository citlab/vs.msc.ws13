package de.tu_berlin.citlab.testsuite.testSkeletons;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.helpers.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 3/11/14.
 */
abstract public class TopologyTest
{
	static {
		System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, System.getProperty("user.dir")+"/master/log4j2-testsuite.xml");
	}

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.TOPOLOGYTEST_ID);
	private static final Marker BASIC = DebugLogger.getBasicMarker();
	private static final Marker DEFAULT = DebugLogger.getDefaultMarker();

    private static final ArrayList<BoltTest> boltTests = new ArrayList<BoltTest>();
    private final TopologySetup topologySetup;

    private BoltEmission lastBoltOutput;

    public TopologyTest()
    {
        List<BoltTestConfig> testTopology = defineTopologySetup();
        topologySetup = new TopologySetup(testTopology);
    }


//Used in @Test
    public void testTopology()
    {
        int n = 0;
        for (String actTestName : topologySetup.boltNameOrder) {
			final UDFBolt testBolt = topologySetup.boltTests.get(actTestName);
			final int sleepBetweenTuples = topologySetup.boltSleepTimer.get(actTestName);
            final OperatorTest actBoltOPTest = topologySetup.boltOPTests.get(actTestName);
            final WindowHandler actBoltWinHandler = testBolt.getWindowHandler();

            BoltTest actBoltTest;
            if(n == 0){ //For the first bolt in the topology:
               try{
                   BoltEmission firstInput = defineFirstBoltsInput();
				   LOGGER.info(DEFAULT, "Initial input of topology's first node is: \n{}", LogPrinter.toTupleListString(firstInput.tupleList));

                    actBoltTest = new BoltTest(actTestName, actBoltOPTest, firstInput.outputFields) {
                        @Override
                        public Window<Tuple, List<Tuple>> initWindow() {
                            return actBoltWinHandler.getStub();
                        }

                        @Override
                        public WindowHandler initWindowHandler() {
                            return actBoltWinHandler;
                        }

						@Override
						public List<List<Object>> assertWindowedOutput(List<Tuple> inputTuples) {
							return actBoltOPTest.assertOperatorOutput(inputTuples);
						}
					};
                    actBoltTest.initTestSetup(firstInput.tupleList);
               }
               catch(NullPointerException e){
                   LOGGER.error(BASIC, "First Bolt Inputs are neeeded to be defined in defineFirstBoltsInput()!", e);
                   return;
               }
            }
            else{
                actBoltTest = new BoltTest(actTestName, actBoltOPTest, testBolt.getOutputFields()) {
                    @Override
                    public Window<Tuple, List<Tuple>> initWindow() {
                        return actBoltWinHandler.getStub();
                    }

                    @Override
                    public WindowHandler initWindowHandler() {
                        return actBoltWinHandler;
                    }

					@Override
					public List<List<Object>> assertWindowedOutput(List<Tuple> inputTuples) {
						return actBoltOPTest.assertOperatorOutput(inputTuples);
					}
				};
                actBoltTest.initTestSetup(lastBoltOutput.tupleList);
            }
            boltTests.add(actBoltTest);
            lastBoltOutput = actBoltTest.testUDFBolt(sleepBetweenTuples);
            n++;
        }
    }

//Used in @AfterClass
    public static void terminateTopology()
    {
		for(BoltTest actTest : boltTests)
		{
			LOGGER.debug("Terminating TestSetup for Bolt: {}...", actTest.testName);
			actTest.terminateTestSetup();
		}

        boltTests.clear();
    }

    abstract protected BoltEmission defineFirstBoltsInput();
    abstract protected List<BoltTestConfig> defineTopologySetup();
}
