package de.tu_berlin.citlab.testsuite.tests.twitter.boltTests;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.UDFBoltTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 1/21/14.
 */
public class Bolt1_WordFlatMap extends UDFBoltTest implements UDFBoltTestMethods
{

    public Bolt1_WordFlatMap(String testName, OperatorTest opTest)
    {
        super(testName, opTest, new Fields("user_id", "word", "id"));
    }

    @Override
    public List<Tuple> generateInputTuples() {
        List<Tuple> tupleList = TupleMockFactory.generateTupleList_ByFields(new Values[]{new Values(1, "hey leute",0),
                                                                                         new Values(1, "sinnvoller Post.", 0),
                                                                                         new Values(1,"bomben bauen macht spass",0)},
                                                                            this.getOpTest().getInputFields());
        //tupleList.add(TupleMock.mockTickTuple());
        return tupleList;
    }

    @Override
    public Window<Tuple, List<Tuple>> initWindow() {
        int windowSize = 2;
        int slidingOffset = 2;
        return new CountWindow<Tuple>(windowSize, slidingOffset);
    }

    @Override
    public IKeyConfig initKeyConfig() {
        return null;
    }

    @Override
    public List<List<Object>> assertOutput(List<Tuple> inputTuples) {
        List<List<Object>> outputVals = new ArrayList<List<Object>>();
        outputVals.add(new Values(1, "hey", 0));
        outputVals.add(new Values(1, "leute", 0));
        outputVals.add(new Values(1, "sinnvoller", 0));
        outputVals.add(new Values(1, "Post.", 0));
        return outputVals;
    }
}
