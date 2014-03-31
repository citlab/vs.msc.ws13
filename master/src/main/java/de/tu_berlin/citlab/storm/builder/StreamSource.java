package de.tu_berlin.citlab.storm.builder;


import de.tu_berlin.citlab.storm.spouts.UDFSpout;
import de.tu_berlin.citlab.storm.udf.UDFOutput;

public class StreamSource extends StreamNode {
    protected UDFSpout spout;
    public StreamSource(StreamBuilder builder) {
        super(builder);
    }
    public StreamSource(StreamBuilder builder, UDFSpout spout ){
        super(builder);
        this.spout = spout;

    }

    @Override
    public UDFOutput getUDFOutput(){
        return spout;
    }

}
