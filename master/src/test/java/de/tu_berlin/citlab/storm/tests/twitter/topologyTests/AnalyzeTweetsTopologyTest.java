package de.tu_berlin.citlab.storm.tests.twitter.topologyTests;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.topologies.AnalyzeTweetsTopology;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.TopologySetup;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Constantin on 12.03.14.
 */
public class AnalyzeTweetsTopologyTest extends TopologyTest
{

    @Override
    protected BoltEmission defineFirstBoltsInput()
    {
        return null;
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
