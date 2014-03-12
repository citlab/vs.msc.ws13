package de.tu_berlin.citlab.storm.tests.twitter.boltTests;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.UDFBoltTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;

import java.util.List;

/**
 * Created by Constantin on 1/21/14.
 */
public class Bolt6_SignUserWithTweets extends UDFBoltTest implements UDFBoltTestMethods
{

    public Bolt6_SignUserWithTweets(String testName, OperatorTest opTest)
    {
        super(testName, opTest, new Fields("user_id", "msg", "total_significance"));
    }

    @Override
    public Window<Tuple, List<Tuple>> initWindow() {
        return null;
    }

    @Override
    public IKeyConfig initWindowHandler() {
        return null;
    }
}
