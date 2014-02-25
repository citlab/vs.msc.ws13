package de.tu_berlin.citlab.testsuite.tests.filterTests;

import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestingEnvironment
{
    private static FilterOperatorTest tOp1;
    private static FilterBoltTest tBolt1;


    public static void initLogger()
    {
//        DebugLogger.setEnabled(true);
//        DebugLogger.setConsoleOutput(DebugLogger.LoD.DEFAULT, true);
//        DebugLogger.appendTimeToOutput(true);
//        DebugLogger.appendCounterToOutput(true);
    }

    @BeforeClass
    public static void initEnvironment()
    {
        initLogger();
        Fields inputFields = new Fields("Key", "Value");
        Fields outputFields = new Fields("Key", "Value");
        tOp1 = new FilterOperatorTest(inputFields);
        tBolt1 = new FilterBoltTest(tOp1, outputFields);
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
