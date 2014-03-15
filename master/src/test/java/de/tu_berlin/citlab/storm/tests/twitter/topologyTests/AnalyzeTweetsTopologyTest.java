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
//        Fields inputFields = new Fields("user", "id", "tweet");
		String[] twitterUsers 	= new String[]{	"Hennes", "4n4rch7", "ReliOnkel", "Matze Maik", "Capt. Nonaim"};
		String[] dictionary 	= new String[]{	"Kartoffel", "Gemüse", "Schnitzel",
												"bombe", "berlin", "gott", "allah",
												"Pilates", "Politik", "Kapital", "Twitter",
												"der", "die", "das"};
		ArrayList<Tuple> twitterTuples = TupleMockFactory.generateTwitterTuples(twitterUsers, dictionary, 10, 100);
        Fields outputFields = new Fields("user", "id", "word");
//        ArrayList<Tuple> firstTuples = TupleMockFactory.generateTupleList_ByFields(
//			new Values[]{	new Values("Hennes", 123, "Wow, ich hab heute wieder einen Vogel gesehen!"),
//							new Values("4n4rch7", 789, "Wie war gleich die Adresse für diese hübsche Bomben Anleitung?"),
//							new Values("ReliOnkel", 666, "Gott ist groß!", 0)},
//                inputFields);

        BoltEmission firstInput = new BoltEmission(twitterTuples, outputFields);
        return firstInput;
    }

    @Override
    protected List<BoltTestConfig> defineTopologySetup() {
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
        final String boltTestName = "FlatMapTweetWords";
        List<List<Object>> assertedOutput = new ArrayList<List<Object>>();
//        assertedOutput.add(new Values(1, "hey", 0));
//        assertedOutput.add(new Values(1, "leute", 0));
//        assertedOutput.add(new Values(1, "heute.", 0));

        return new BoltTestConfig(boltTestName, testingBolt, assertedOutput);
    }

    private BoltTestConfig testStaticHashJoin(UDFBolt testingBolt) {
        final String boltTestName = "createStaticHashJoin";
        List<List<Object>> assertedOutput = new ArrayList<List<Object>>();
        assertedOutput.add(new Values(1, "hey", 0));
        assertedOutput.add(new Values(1, "leute", 0));
        assertedOutput.add(new Values(1, "heute.", 0));

        return new BoltTestConfig(boltTestName, testingBolt, assertedOutput);
    }

    private BoltTestConfig testUserSign(UDFBolt testingBolt) {
        final String boltTestName = "createStaticHashJoin";
        List<List<Object>> assertedOutput = new ArrayList<List<Object>>();
        assertedOutput.add(new Values(1, "hey", 0));
        assertedOutput.add(new Values(1, "leute", 0));
        assertedOutput.add(new Values(1, "heute.", 0));

        return new BoltTestConfig(boltTestName, testingBolt, assertedOutput);
    }

}
