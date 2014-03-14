package de.tu_berlin.citlab.storm.tests.twitter.topologyTests;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.topologies.AnalyzeTweetsTopology;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory;
import de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 12.03.14.
 */
public class AnalyzeTweetsTopologyTest extends TopologyTest
{
    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, System.getProperty("user.dir")+"/master/log4j2-testsuite.xml");
    }
    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.TOPOLOGYTEST_ID);

    @Override
    protected BoltEmission defineFirstBoltsInput()
    {
        Fields inputFields = new Fields("user_id", "message", "id");
        Fields outputFields = new Fields("user_id", "word", "id");
        ArrayList<Tuple> firstTuples = TupleMockFactory.generateTupleList_ByFields(
                new Values[]{new Values(1, "hey leute", 0),
                        new Values(1, "sinnvoller Post.", 0),
                        new Values(1, "bomben bauen macht spass", 0)},
                inputFields);

        BoltEmission firstInput = new BoltEmission(firstTuples, outputFields);
        return firstInput;
    }

    @Override
    protected List<BoltTestConfig> defineTopologySetup() {
        AnalyzeTweetsTopology topology = new AnalyzeTweetsTopology();

        List<BoltTestConfig> testTopology = new ArrayList<BoltTestConfig>();

        final UDFBolt flatMapTweetWords = topology.flatMapTweetWords();
        final String flatMapName = "FlatMapTweetWords";
        List<List<Object>> assertedOutput = new ArrayList<List<Object>>(); //TODO: assert something...
        BoltTestConfig flatMapTest = new BoltTestConfig(flatMapName, flatMapTweetWords, assertedOutput);
        testTopology.add(flatMapTest);

        return testTopology;
    }

    @Test
    public void testTopology()
    {
        super.testTopology();
    }

    @AfterClass
    public static void terminateEnvironment()
    {
        terminateTopology();
    }

}
