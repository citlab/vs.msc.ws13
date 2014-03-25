package de.tu_berlin.citlab.storm.builder;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.udf.IOperator;

/**
 * Created by kay on 3/25/14.
 */
public class StreamSink extends UDFBolt {

    public StreamSink(Fields outputFields, IOperator operator) {
        super(outputFields, operator);
    }
}
