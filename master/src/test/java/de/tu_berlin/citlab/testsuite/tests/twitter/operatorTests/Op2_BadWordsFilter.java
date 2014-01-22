package de.tu_berlin.citlab.testsuite.tests.twitter.operatorTests;


import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;
import de.tu_berlin.citlab.testsuite.tests.twitter.helpers.STORAGE;
import de.tu_berlin.citlab.testsuite.tests.twitter.helpers.BadWord;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Constantin on 1/21/14.
 */
public class Op2_BadWordsFilter extends OperatorTest implements OperatorTestMethods
{
    public Op2_BadWordsFilter(String testName) {
        super(testName, new Fields("user_id", "word", "id"));
    }

    @Override
    public List<Tuple> generateInputTuples() {
        List<Tuple> tupleList = new ArrayList<Tuple>();
        int userID = 1;
        int id = 0;
        tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "hey", id), this.getInputFields()));
        tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "leute", id), this.getInputFields()));
        tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "heute.", id), this.getInputFields()));
        return tupleList;
    }

    @Override
    public IOperator initOperator(List<Tuple> inputTuples) {
        IOperator flatMap = new IOperator(){

            public void execute(List<Tuple> input, OutputCollector collector) {
                for(Tuple t : input){
                    String word = t.getValueByField("word").toString().toLowerCase();

                    // do not change
                    if( STORAGE.badWords.containsKey(word) ){
                        BadWord badWord = STORAGE.badWords.get(word);
                        collector.emit( new Values( t.getValueByField("user_id"), word, t.getValueByField("id"), ""+badWord.significance ) );
                    }//if

                }//for

            }// execute()
        };
        return flatMap;
    }

    @Override
    public List<List<Object>> assertOutput(List<Tuple> inputTuples) {
        List<List<Object>> emptyList = new ArrayList<List<Object>>();
        return emptyList;
    }
}
