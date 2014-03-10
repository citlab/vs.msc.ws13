package de.tu_berlin.citlab.storm.tests.twitter;

import de.tu_berlin.citlab.storm.tests.twitter.boltTests.*;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.BadWord;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.STORAGE;
import de.tu_berlin.citlab.storm.tests.twitter.operatorTests.*;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
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

	private static Op3_UpdateUserSign tOp3;
	private static Bolt3_UpdateUserSign tBolt3;

	private static Op4_UserTotalSign tOp4;
	private static Bolt4_UserTotalSign tBolt4;

	private static Op5_SignificantUsers tOp5;
	private static Bolt5_SignificantUsers tBolt5;

	private static Op6_SignUserWithTweets tOp6;
	private static Bolt6_SignUserWithTweets tBolt6;



    @BeforeClass
    public static void initEnvironment()
    {
        tOp1 = new Op1_WordFlatMap("OP1_WordsFlat");
        tBolt1 = new Bolt1_WordFlatMap("Bolt1_WordsFlat",tOp1);
        tOp2 = new Op2_BadWordsFilter("OP2_BadWordsFilter");
        tBolt2 = new Bolt2_BadWordsFilter("Bolt2_BadWordsFilter", tOp2);
		tOp3 = new Op3_UpdateUserSign("OP3_UpdateUserSign");
		tBolt3 = new Bolt3_UpdateUserSign("Bolt3_UpdateUserSign",tOp3);
		tOp4 = new Op4_UserTotalSign("OP4_UserTotalSign");
		tBolt4 = new Bolt4_UserTotalSign("Bolt4_UserTotalSign", tOp4);
		tOp5 = new Op5_SignificantUsers("Op5_SignificantUsers");
		tBolt5 = new Bolt5_SignificantUsers("Bolt5_SignificantUsers", tOp5);
		tOp6 = new Op6_SignUserWithTweets("Op6_SignUserWithTweets");
		tBolt6 = new Bolt6_SignUserWithTweets("Bolt6_SignUserWithTweets", tOp6);

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
		tOp3.terminateTestSetup();
		tBolt3.terminateTestSetup();
		tOp4.terminateTestSetup();
		tBolt4.terminateTestSetup();
		tOp5.terminateTestSetup();
		tBolt5.terminateTestSetup();
		tOp6.terminateTestSetup();
		tBolt6.terminateTestSetup();
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
	public void testUpdateUserSign()
	{
		tOp3.testOperator();
		tBolt3.testUDFBolt();
	}

	@Test
	public void testUserTotalSign()
	{
		tOp4.testOperator();
		tBolt4.testUDFBolt();
	}

	@Test
	public void testSignificantUsers()
	{
		tOp5.testOperator();
		tBolt5.testUDFBolt();
	}

	@Test
	public void testSignUserWithTweets()
	{
		tOp6.testOperator();
		tBolt6.testUDFBolt();
	}

    @Test
    public void testLogger()
    {
//        LogManager.
        assertTrue(LOGGER.isDebugEnabled());
//        assertTrue(LOGGER.isTraceEnabled());
    }
}
