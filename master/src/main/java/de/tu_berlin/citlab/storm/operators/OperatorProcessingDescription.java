package de.tu_berlin.citlab.storm.operators;

import de.tu_berlin.citlab.storm.udf.IOperator;

import java.io.Serializable;

public class OperatorProcessingDescription implements Serializable {

    private IOperator operator;
    private String source;

    public OperatorProcessingDescription(IOperator operator, String source) {
        this.operator = operator;
        this.source = source;
    }

    public IOperator getOperator(){ return  operator; }
    public String getSource(){ return source; }
}
