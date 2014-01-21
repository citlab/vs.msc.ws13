package de.tu_berlin.citlab.testsuite.tests.twitter;

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
class Op1_WordFlatMap extends OperatorTest implements OperatorTestMethods
{

    public Op1_WordFlatMap(String testName, Fields inputFields) {
        super(testName, inputFields);
    }

    @Override
    public List<Tuple> generateInputTuples() {
        List<Tuple> tupleList = new ArrayList<Tuple>();
        tupleList.add(TupleMock.mockTupleByFields(new Values("hey leute heute war angela total witzlos."), this.getInputFields()));
        return tupleList;
    }

    @Override
    public IOperator initOperator(List<Tuple> inputTuples) {
        IOperator flatMap = new IOperator(){
            public void execute(List<Tuple> input, OutputCollector collector) {
                for(Tuple t : input){
                    //TODO: check why getValueByField("msg") is null.
                    String[] words = t.getValueByField("msg").toString().split(" ");
                    for( String word : words ){

                        collector.emit( new Values( t.getValueByField("user_id"), word, t.getValueByField("id") ) );
                    }//for
                }//for

            }// execute()
        };
        return flatMap;
    }

    @Override
    public List<List<Object>> assertOutput(List<Tuple> inputTuples) {
        return null;
    }
}
