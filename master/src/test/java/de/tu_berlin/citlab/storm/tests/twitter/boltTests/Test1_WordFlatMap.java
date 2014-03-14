package de.tu_berlin.citlab.storm.tests.twitter.boltTests;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory;
import de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.StandaloneTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 1/21/14.
 */
public class Test1_WordFlatMap extends StandaloneTest<BoltTest_WordFlatMap, OpTest_WordFlatMap>
{
    public static final String testName ="WordFlatMap";
    private static final Fields inputFields = new Fields("user_id", "message", "id");
    private static final Fields outputFields = new Fields("user_id", "word", "id");

    @Override
    protected BoltTest_WordFlatMap initBoltTestDescr() {
        return new BoltTest_WordFlatMap(testName, outputFields);
    }

    @Override
    protected OpTest_WordFlatMap initOpTestDescr() {
        return new OpTest_WordFlatMap(testName);
    }

    @Override
    protected List<Tuple> generateInputTuples() {
        return TupleMockFactory.generateTupleList_ByFields(
                new Values[]{new Values(1, "hey leute", 0),
                        new Values(1, "sinnvoller Post.", 0),
                        new Values(1, "bomben bauen macht spass", 0)},
                inputFields);
    }
}


class BoltTest_WordFlatMap extends BoltTest implements UDFBoltTestMethods
{


    public BoltTest_WordFlatMap(String testName, Fields outputFields)
    {
        super(testName, new OpTest_WordFlatMap(testName), outputFields);
    }

    @Override
    public Window<Tuple, List<Tuple>> initWindow() {
        int windowSize = 2;
        int slidingOffset = 2;
        return new CountWindow<Tuple>(windowSize, slidingOffset);
    }

    @Override
    public WindowHandler initWindowHandler() {
        return null;
    }

    @Override
    public List<List<Object>> assertWindowedOutput(List<Tuple> inputTuples) {
        List<List<Object>> outputVals = new ArrayList<List<Object>>();
        outputVals.add(new Values(1, "hey", 0));
        outputVals.add(new Values(1, "leute", 0));
        outputVals.add(new Values(1, "sinnvoller", 0));
        outputVals.add(new Values(1, "Post.", 0));
        return outputVals;
    }
}

class OpTest_WordFlatMap extends OperatorTest
{

    public OpTest_WordFlatMap(String testName) {
        super(testName);
    }


    @Override
    public List<Tuple> generateInputTuples() {
        return null;
    }

    @Override
    public IOperator initOperator(List<Tuple> inputTuples) {
        IOperator flatMap = new IOperator(){
            public void execute(List<Tuple> input, OutputCollector collector) {
                for(Tuple t : input){
                    String[] words = t.getValueByField("msg").toString().split(" ");
                    for( String word : words ){
                        collector.emit(new Values( t.getValueByField("user_id"),
                                word, t.getValueByField("id") ) );
                    }//for
                }//for

            }// execute()
        };
        return flatMap;
    }

    @Override
    public List<List<Object>> assertOperatorOutput(List<Tuple> inputTuples) {
        List<List<Object>> outputVals = new ArrayList<List<Object>>();
        outputVals.add(new Values(1, "hey", 0));
        outputVals.add(new Values(1, "leute", 0));
        outputVals.add(new Values(1, "heute.", 0));
        return outputVals;
    }
}
