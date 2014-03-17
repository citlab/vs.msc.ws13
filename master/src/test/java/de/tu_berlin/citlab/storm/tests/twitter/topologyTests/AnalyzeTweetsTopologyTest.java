package de.tu_berlin.citlab.storm.tests.twitter.topologyTests;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.topologies.AnalyzeTweetsTopology;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory;
import de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 12.03.14.
 */
public class AnalyzeTweetsTopologyTest extends TopologyTest
{
    @Override
    public BoltEmission defineFirstBoltsInput()
    {
		String[] twitterUsers 	= new String[]{	"Hennes", "4n4rch7", "ReliOnkel", "Matze Maik", "Capt. Nonaim"};
		String[] dictionary 	= new String[]{	"Kartoffel", "Gemüse", "Schnitzel",
												"bombe", "berlin", "gott", "allah",
												"Pilates", "Politik", "Kapital", "Twitter",
												"der", "die", "das"};
		ArrayList<Tuple> twitterTuples = TupleMockFactory.generateTwitterTuples(twitterUsers, dictionary, 5, 12);
        BoltEmission firstInput = new BoltEmission(twitterTuples);
        return firstInput;
    }

    @Override
    public List<BoltTestConfig> defineTopologySetup() {
        AnalyzeTweetsTopology topology = new AnalyzeTweetsTopology();
        List<BoltTestConfig> testTopology = new ArrayList<BoltTestConfig>();

        final UDFBolt flatMapTweetWords = topology.flatMapTweetWords();
        BoltTestConfig flatMapTest = testMapTweetWords(flatMapTweetWords);
        testTopology.add(flatMapTest);

        final UDFBolt staticHashJoin = topology.createStaticHashJoin();
        BoltTestConfig hashJoinTest = testStaticHashJoin(staticHashJoin);
        testTopology.add(hashJoinTest);

        final UDFBolt reduceUserSign = topology.reduceUserSignificance();
        BoltTestConfig userSignTest = testUserSign(reduceUserSign);
        testTopology.add(userSignTest);

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



/* Topology Test-Configs: */
/* ====================== */

    private BoltTestConfig testMapTweetWords(UDFBolt testingBolt) {
        final String boltTestName = "flatmap_tweet_words";
//        List<List<Object>> assertedOutput = new ArrayList<List<Object>>();
//        assertedOutput.add(new Values("Name", 123, "Twitter msg."));

        return new BoltTestConfig(boltTestName, testingBolt, 0, null);
    }

    private BoltTestConfig testStaticHashJoin(UDFBolt testingBolt) {
        final String boltTestName = "join_with_badwords";
        return new BoltTestConfig(boltTestName, testingBolt, 0, null);
    }

    private BoltTestConfig testUserSign(UDFBolt testingBolt) {
        final String boltTestName = "reduce_to_user_significance";
        return new BoltTestConfig(boltTestName, testingBolt, 200, null);
    }

}
