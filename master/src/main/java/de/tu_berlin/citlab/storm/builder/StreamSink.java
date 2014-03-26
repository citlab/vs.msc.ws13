package de.tu_berlin.citlab.storm.builder;


import de.tu_berlin.citlab.storm.udf.IOperator;

public class StreamSink extends StreamNode {
    private IOperator operator;
    public StreamSink(StreamBuilder builder, IOperator operator) {
        super(builder);
        this.operator=operator;
    }

    public IOperator getOperator(){
        return operator;
    }
}
