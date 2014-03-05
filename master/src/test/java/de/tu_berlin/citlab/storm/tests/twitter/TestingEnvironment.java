package de.tu_berlin.citlab.storm.tests.twitter;

import de.tu_berlin.citlab.storm.tests.twitter.boltTests.Bolt1_WordFlatMap;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.BadWord;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.STORAGE;
import de.tu_berlin.citlab.storm.tests.twitter.operatorTests.Op1_WordFlatMap;
import de.tu_berlin.citlab.storm.tests.twitter.operatorTests.Op2_BadWordsFilter;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.storm.tests.twitter.boltTests.Bolt2_BadWordsFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;


public class TestingEnvironment
{
    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, System.getProperty("user.dir")+"/master/log4j2-testsuite.xml");
    }
    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.BOLTTEST_ID);

    private static Op1_WordFlatMap tOp1;
    private static Bolt1_WordFlatMap tBolt1;

    private static Op2_BadWordsFilter tOp2;
    private static Bolt2_BadWordsFilter tBolt2;



    @BeforeClass
    public static void initEnvironment()
    {
        tOp1 = new Op1_WordFlatMap("OP1_WordsFlat");
        tBolt1 = new Bolt1_WordFlatMap("Bolt1_WordsFlat",tOp1);
        tOp2 = new Op2_BadWordsFilter("OP2_BadWordsFilter");
        tBolt2 = new Bolt2_BadWordsFilter("Bolt2_BadWordsFilter", tOp2);

        STORAGE.badWords.put("bombe", new BadWord("bombe", 100));
        STORAGE.badWords.put("nuklear", new BadWord("nuklear", 1000));
        STORAGE.badWords.put("anschlag", new BadWord("anschlag", 200));
        STORAGE.badWords.put("religion", new BadWord("religion", 100));
        STORAGE.badWords.put("macht", new BadWord("macht", 300));
        STORAGE.badWords.put("kampf", new BadWord("kampf", 300));
    }

    @AfterClass
    public static void terminateEnvironment()
    {
        tOp1.terminateTestSetup();
        tBolt1.terminateTestSetup();
        tOp2.terminateTestSetup();
        tBolt2.terminateTestSetup();
    }


    @Test
    public void testWordFlatMap()
    {
        tOp1.testOperator();
        tBolt1.testUDFBolt();
    }

    @Test
    public void testBadWordsFilter()
    {
        tOp2.testOperator();
        tBolt2.testUDFBolt();
    }

    @Test
    public void testLogger()
    {
//        LogManager.
        assertTrue(LOGGER.isDebugEnabled());
//        assertTrue(LOGGER.isTraceEnabled());
    }
}
