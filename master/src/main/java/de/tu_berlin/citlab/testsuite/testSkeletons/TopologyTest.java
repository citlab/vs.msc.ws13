package de.tu_berlin.citlab.testsuite.testSkeletons;

import java.util.ArrayList;

/**
 * Created by Constantin on 3/11/14.
 */
public class TopologyTest
{
    private final ArrayList<UDFBoltTest> boltTests;

    public TopologyTest(UDFBoltTest... boltTests)
    {
        this.boltTests = new ArrayList<UDFBoltTest>(boltTests.length);

        for(UDFBoltTest actTest : boltTests){
            this.boltTests.add(actTest);
        }
    }

    //TODO: go on here. Pass output of first bolt test to second bolt test, usw.
}
