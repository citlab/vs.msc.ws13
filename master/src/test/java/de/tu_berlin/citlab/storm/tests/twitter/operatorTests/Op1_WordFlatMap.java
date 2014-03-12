package de.tu_berlin.citlab.storm.tests.twitter.operatorTests;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 1/21/14.
 */
public class Op1_WordFlatMap extends OperatorTest implements OperatorTestMethods
{

    public Op1_WordFlatMap(String testName) {
        super(testName, new Fields("user_id", "msg", "id"));
    }

    @Override
    public List<Tuple> generateInputTuples() {
        List<Tuple> tupleList = new ArrayList<Tuple>();
        int userID = 1;
        int id = 0;
        tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "hey leute heute.", id),
                                                this.getInputFields()));
        return tupleList;
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
