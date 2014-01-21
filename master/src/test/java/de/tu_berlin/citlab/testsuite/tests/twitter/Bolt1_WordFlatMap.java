package de.tu_berlin.citlab.testsuite.tests.twitter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.UDFBoltTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;

import java.util.List;

/**
 * Created by Constantin on 1/21/14.
 */
public class Bolt1_WordFlatMap extends UDFBoltTest implements UDFBoltTestMethods
{

    public Bolt1_WordFlatMap(String testName, OperatorTest opTest, Fields outputFields)
    {
        super(testName, opTest, outputFields);
    }

    @Override
    public Window<Tuple, List<Tuple>> initWindow() {
        return null;
    }

    @Override
    public IKeyConfig initKeyConfig() {
        return null;
    }
}
