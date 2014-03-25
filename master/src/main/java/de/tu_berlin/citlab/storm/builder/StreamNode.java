package de.tu_berlin.citlab.storm.builder;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.operators.Filter;
import de.tu_berlin.citlab.storm.operators.FlatMapper;
import de.tu_berlin.citlab.storm.operators.Mapper;
import de.tu_berlin.citlab.storm.operators.Reducer;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.window.IKeyConfig;

import java.util.Iterator;
import java.util.List;

abstract public class StreamNode {
    public List<StreamNode> sources;
    private UDFBolt bolt;
    private StreamBuilder builder;

    public StreamNode(StreamBuilder builder){
        this.builder = builder;
    }

    public StreamNode merge( StreamNode ... nodes ){
        return this;
    }
    public StreamBuilder getStreamBuilder(){
        return builder;
    }

    public StreamNode join( IKeyConfig groupKey,
                            Iterator<Tuple> tuples,
                            TupleProjection projection,
                            Fields outputFields ){
        return this;
    }
    public StreamNode join( StreamNode ... nodes ){
        return this;
    }
    public StreamNode filter( Filter filter ){
        return this;
    }
    public StreamNode map( Mapper mapper ){
        return this;
    }
    public StreamNode flapMap( FlatMapper flatmapper, Fields outputFields ){
        return this;
    }
    public StreamNode cassnadraSink(){
        return this;
    }
    public StreamNode reduce( Reducer reducer ){
        return this;
    }
    public StreamNode caseInput( Reducer reducer ){
        return this;
    }

    public String getNodeId(){
        return this.getClass().getSimpleName()+"_"+this.getClass().hashCode();

    }
}
