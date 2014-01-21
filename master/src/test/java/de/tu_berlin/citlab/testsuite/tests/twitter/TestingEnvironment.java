package de.tu_berlin.citlab.testsuite.tests.twitter;

import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.tests.filterTests.FilterBoltTest;
import de.tu_berlin.citlab.testsuite.tests.filterTests.FilterOperatorTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestingEnvironment
{
    private static Op1_WordFlatMap tOp1;
    private static Bolt1_WordFlatMap tBolt1;


    public static void initLogger()
    {
        DebugLogger.setEnabled(true);
        DebugLogger.setConsoleOutput(DebugLogger.LoD.DEFAULT, true);
        DebugLogger.appendTimeToOutput(true);
        DebugLogger.appendCounterToOutput(true);
    }



    @BeforeClass
    public static void initEnvironment()
    {
        initLogger();
        tOp1 = new Op1_WordFlatMap("OP1_WordsFlat", new Fields("msg"));
        tBolt1 = new Bolt1_WordFlatMap("Bolt1_WordsFlat",tOp1, new Fields("user_id", "word", "id"));
    }

    @AfterClass
    public static void terminateEnvironment()
    {
        tOp1.terminateTestSetup();
        tBolt1.terminateTestSetup();
    }


    @Test
    public void testFilter()
    {
        tOp1.testOperator();
        tBolt1.testUDFBolt();
    }
}
