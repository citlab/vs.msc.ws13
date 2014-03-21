package de.tu_berlin.citlab.storm.operators;

import de.tu_berlin.citlab.storm.udf.IOperator;

import java.io.Serializable;

public class OperatorProcessingDescription implements Serializable {

    private IOperator operator;
    private String[] sources;

    public OperatorProcessingDescription(IOperator operator, String ... sources) {
        this.operator = operator;
        this.sources = sources;
    }

    public IOperator getOperator(){ return  operator; }
    public String[] getSources(){ return sources; }
}
