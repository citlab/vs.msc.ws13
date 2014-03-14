package de.tu_berlin.citlab.testsuite.testSkeletons;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.TopologySetup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 3/11/14.
 */
abstract public class TopologyTest
{
    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.TOPOLOGYTEST_ID);

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
            final OperatorTest actBoltOPTest = topologySetup.boltOPTests.get(actTestName);
            final WindowHandler actBoltWinHandler = topologySetup.boltWindowHandler.get(actTestName);

            BoltTest actBoltTest;
            if(n == 0){ //For the first bolt in the topology:
               try{
                   BoltEmission firstInput = defineFirstBoltsInput();
                    actBoltTest = new BoltTest(actTestName, actBoltOPTest, firstInput.outputFields) {
                        @Override
                        public Window<Tuple, List<Tuple>> initWindow() {
                            return actBoltWinHandler.getStub();
                        }

                        @Override
                        public WindowHandler initWindowHandler() {
                            return actBoltWinHandler;
                        }
                    };
                    actBoltTest.initTestSetup(firstInput.tupleList);
               }
               catch(NullPointerException e){
                   LOGGER.error("First Bolt Inputs are neeeded to be defined in defineFirstBoltsInput()!", e);
                   return;
               }
            }
            else{
                actBoltTest = new BoltTest(actTestName, actBoltOPTest, lastBoltOutput.outputFields) {
                    @Override
                    public Window<Tuple, List<Tuple>> initWindow() {
                        return actBoltWinHandler.getStub();
                    }

                    @Override
                    public WindowHandler initWindowHandler() {
                        return actBoltWinHandler;
                    }
                };
                actBoltTest.initTestSetup(lastBoltOutput.tupleList);
            }
            boltTests.add(actBoltTest);
            lastBoltOutput = actBoltTest.testUDFBolt();
            n++;
        }
    }

//Used in @AfterClass
    public static void terminateTopology()
    {
       for(BoltTest actTest : boltTests)
       {
            actTest.terminateTestSetup();
       }

        boltTests.clear();
    }

    abstract protected BoltEmission defineFirstBoltsInput();
    abstract protected List<BoltTestConfig> defineTopologySetup();
}
