package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;


public class IdentityOperator extends IOperator {
    public void execute(List<Tuple> input, OutputCollector collector) throws OperatorException {
        for (Tuple t : input) collector.emit(t.getValues());
    }
}
