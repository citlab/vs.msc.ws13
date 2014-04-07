package de.tu_berlin.citlab.testsuite.testSkeletons;


import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.helpers.*;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.TopologyTestMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * <p>
 *     The <b>TopologyTest</b> is an <em><b>abstract Test-Skeleton</b></em> whose implementation class is representing a
 *     test case of several {@link BoltTest BoltTests} in a linear topology chain. This skeleton-class is not able
 *     to represent topologies with different branches in a Bolt-output grouping in one test, but such topologies could
 *     be recreated with several TopologyTests, each for one branch in the shuffleOutput.
 * </p>
 * <p>
 *     In order to sucessfully pass such a test, each {@link BoltTest#assertWindowedOutput(java.util.List)}
 *     have to be equal to the respective output of the of the same BoltTests
 *     {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollector}'s emission for the whole chain. <br />
 *     However, as this could be hard to archieve, using {@link TupleMockFactory}-methods, the assertedOutput may be
 *     set to <b>null</b> for each {@link BoltTestConfig}, which is representing a single Bolt in the TestTopology.
 * </p>
 * <p>
 *     In contrary to the {@link BoltTest} and the {@link OperatorTest}, implementations of this test-skeleton are
 *     fully defining a testing environment. In regards to an environment, only {@link StandaloneTest StandaloneTests}
 *     and {@link TopologyTest TopologyTests} are fully conform with the testing compliance.
 * </p>
 * @author Constantin on 3/11/14.
 */
abstract public class TopologyTest implements TopologyTestMethods
{

/* Global Private Constants: */
/* ========================= */

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.TOPOLOGYTEST_ID);
	private static final Marker BASIC = DebugLogger.getBasicMarker();
	private static final Marker DEFAULT = DebugLogger.getDefaultMarker();

    private static final ArrayList<BoltTest> boltTests = new ArrayList<BoltTest>();
    private final TopologySetup topologySetup;
    private final String topologyTestName;


/* Global Private Variables: */
/* ========================= */

    private BoltEmission lastBoltOutput;


/* The Constructor: */
/* ================ */

    public TopologyTest()
    {
        List<BoltTestConfig> testTopology = defineTopologySetup();
        this.topologyTestName = nameTopologyTest();
        System.setProperty("logTestName", topologyTestName);

        org.apache.logging.log4j.core.LoggerContext ctx =
                (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        ctx.reconfigure();

        topologySetup = new TopologySetup(topologyTestName, testTopology);
    }



/* Public JUnit Methods: */
/* ===================== */

//Used in @Test
	/**
	 * The {@link TopologyTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through a partially
	 * complete UnitTest-lifecycle. This includes a <em><b>test-run</b></em> and the <em>test-termination</em>. <br />
	 * <em>Being a part of the JUnit lifecycle, this method is used in a <b>@Test method</b>.</em>
	 * <p>
	 *	   This test-method is testing each {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}, linked to this TopologyTest.
	 *	   For the whole chain of BoltTests, each bolt executes the inputTuples of the predecessor's
	 *	   {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock}. <br />
	 *	   The first Bolt's input is defined by the {@link de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.TopologyTestMethods#defineFirstBoltsInput()}-method.
	 * </p>
	 */
    public void testTopology()
    {
        int n = 0;
        for (String actTestName : topologySetup.boltNameOrder) {
			final UDFBolt testBolt = topologySetup.boltTests.get(actTestName);
			final int sleepBetweenTuples = topologySetup.boltSleepTimer.get(actTestName);
            final OperatorTest actBoltOPTest = topologySetup.boltOPTests.get(actTestName);
            final WindowHandler actBoltWinHandler = testBolt.getWindowHandler();

			LOGGER.info(DEFAULT, "Defining BoltTest {}", actTestName);
			BoltTest actBoltTest = new BoltTest(actTestName, actBoltOPTest, testBolt.getOutputFields()) {
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

            if(n == 0){ //For the first bolt in the topology:
               try{
                   BoltEmission firstInput = defineFirstBoltsInput();
                   actBoltTest.initTestSetup(firstInput.tupleList);
               }
               catch(NullPointerException e){
                   LOGGER.error(BASIC, "First Bolt Inputs are neeeded to be defined in defineFirstBoltsInput()!", e);
                   return;
               }
            }
            else{
                actBoltTest.initTestSetup(lastBoltOutput.tupleList);
            }
            boltTests.add(actBoltTest);
            lastBoltOutput = actBoltTest.testUDFBolt(sleepBetweenTuples);
            n++;
        }
    }

//Used in @AfterClass
	/**
	 * The {@link TopologyTest} is implemented as a <b>JUnit</b>-TestSkeleton and thus runs through a partially
	 * complete UnitTest-lifecycle. This includes a <em>test-run</em> and the <em><b>test-termination</b></em>. <br />
	 * <em>Being a part of the JUnit lifecycle, this method is used in a <b>@AfterClass method</b>.</em>
	 * <p>
	 *     This method terminates (sets to <b>null</b>) every object from the
	 *     {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest BoltTest}-chain that was set up for the test.
	 * </p>
	 */
    public static void terminateTopology()
    {
		for(BoltTest actTest : boltTests)
		{
			LOGGER.debug("Terminating TestSetup for Bolt: {}...", actTest.testName);
			actTest.terminateTestSetup();
		}

        boltTests.clear();
    }
}
