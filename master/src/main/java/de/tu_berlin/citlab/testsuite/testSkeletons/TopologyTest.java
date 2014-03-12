package de.tu_berlin.citlab.testsuite.testSkeletons;

import java.util.ArrayList;

import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;

/**
 * Created by Constantin on 3/11/14.
 */
abstract public class TopologyTest
{
    private static final ArrayList<UDFBoltTest> boltTests = new ArrayList<UDFBoltTest>();

    public TopologyTest(UDFBoltTest... boltTests)
    {

        for(UDFBoltTest actTest : boltTests){
            this.boltTests.add(actTest);
        }
    }


//Used in @Test
    public void testTopology()
    {
        //Put first boltTest
//        boltTests.get(0)
        for(UDFBoltTest actTest : boltTests){
            BoltEmission boltEmission = actTest.testUDFBolt();
        }
    }

//Used in @AfterClass
    public static void terminateEnvironment()
    {
       for(UDFBoltTest actTest : boltTests)
       {
            actTest.terminateTestSetup();
       }
    }

    abstract BoltEmission defineFirstBoltsInput();
}
