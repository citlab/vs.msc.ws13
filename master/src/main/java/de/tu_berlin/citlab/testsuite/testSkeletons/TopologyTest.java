package de.tu_berlin.citlab.testsuite.testSkeletons;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.TopologySetup;

/**
 * Created by Constantin on 3/11/14.
 */
abstract public class TopologyTest
{
    private static final ArrayList<UDFBoltTest> boltTests = new ArrayList<UDFBoltTest>();
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

            UDFBoltTest actBoltTest;
            if(n == 0){ //For the first bolt in the topology:
                actBoltTest = new UDFBoltTest(actTestName, actBoltOPTest, defineFirstBoltsInput()) {
                    @Override
                    public Window<Tuple, List<Tuple>> initWindow() {
                        return actBoltWinHandler.getStub();
                    }

                    @Override
                    public WindowHandler initWindowHandler() {
                        return actBoltWinHandler;
                    }
                };
            }
            else{
                actBoltTest = new UDFBoltTest(actTestName, actBoltOPTest, lastBoltOutput) {
                    @Override
                    public Window<Tuple, List<Tuple>> initWindow() {
                        return actBoltWinHandler.getStub();
                    }

                    @Override
                    public WindowHandler initWindowHandler() {
                        return actBoltWinHandler;
                    }
                };
            }
            boltTests.add(actBoltTest);
            lastBoltOutput = actBoltTest.testUDFBolt();
            n++;
        }
    }

//Used in @AfterClass
    public static void terminateTopology()
    {
       for(UDFBoltTest actTest : boltTests)
       {
            actTest.terminateTestSetup();
       }

        boltTests.clear();
    }

    abstract protected BoltEmission defineFirstBoltsInput();
    abstract protected List<BoltTestConfig> defineTopologySetup();
}
