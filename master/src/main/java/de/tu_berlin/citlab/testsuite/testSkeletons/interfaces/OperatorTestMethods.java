package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;

/**
 * Created by Constantin on 1/20/14.
 */
public interface OperatorTestMethods
{
    public List<Tuple> generateInputTuples();

    public IOperator initOperator(final List<Tuple> inputTuples);

    public List<List<Object>> assertOperatorOutput(final List<Tuple> inputTuples);
}
