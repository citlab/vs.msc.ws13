package de.tu_berlin.citlab.storm.builder;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.*;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TupleComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamNode {
    public static int StreamNodeCounter = 1;
    public List<StreamNode> sources;
    private UDFBolt bolt;
    private StreamBuilder builder;
    private String nodeId;

    public StreamNode(StreamBuilder builder){
        StreamNodeCounter++;
        nodeId = this.getClass().getSimpleName()+"-"+this.getClass().hashCode()+"-"+StreamNodeCounter;
        this.builder = builder;
    }

    public StreamNode merge( StreamNode ... nodes ){
        return this;
    }
    public StreamBuilder getStreamBuilder(){
        return builder;
    }

    public StreamJoin join( /*Fields groupKey,*/
                            Iterator<Tuple> tuples,
                            TupleProjection projection,
                            TupleComparator comparator,
                            Fields outputFields ){

        StreamJoin node = new StreamJoin( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
            new UDFBolt(
                    outputFields,
                new StaticHashJoinOperator(
                        comparator,
                        projection,
                        tuples ),
                    getStreamBuilder().getDefaultWindowType(),
                KeyConfigFactory.BySource()
        ) )
        .shuffleGrouping(this.getNodeId());
         return node;
    }

    /*
    public StreamNode join( StreamNode ... nodes ){
        return this;
    }*/
    public StreamFilter filter( Filter filter, Fields outputFields  ){
        StreamFilter node = new StreamFilter( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
                new UDFBolt(
                        outputFields,
                        new FilterOperator(filter),
                        getStreamBuilder().getDefaultWindowType()
                ))
        .shuffleGrouping( this.getNodeId());
        
        return node;
    }
    public StreamMapper map( Mapper mapper, Fields outputFields  ){
        StreamMapper node = new StreamMapper( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt( node.getNodeId(),
           new UDFBolt(
                   outputFields,
                new MapOperator(mapper),
                getStreamBuilder().getDefaultWindowType()
            ) )
            .shuffleGrouping(this.getNodeId());

        return node;
    }
    public StreamFlatMapper flapMap( FlatMapper flatmapper, Fields outputFields ){
        StreamFlatMapper node = new StreamFlatMapper( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt( node.getNodeId(),
                new UDFBolt(
                        outputFields,
                        new FlatMapOperator(flatmapper),
                        getStreamBuilder().getDefaultWindowType()
                ) )
                .shuffleGrouping( this.getNodeId() );
        return node;
    }
    public void save( StreamSink sink ){

    }

    public StreamReducer reduce( Fields reduceKey, Reducer reducer, Object init, Fields outputFields ){
        StreamReducer node = new StreamReducer( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt( node.getNodeId(),
            new UDFBolt(
                outputFields,
                new ReduceOperator( reduceKey, reducer, init /*reducer init value */ ),
                getStreamBuilder().getDefaultWindowType(),
                KeyConfigFactory.ByFields( reduceKey )
        ) ).fieldsGrouping( this.getNodeId(), reduceKey );
        return node;
    }
    public StreamNode caseInput( Reducer reducer ){
        return this;
    }
    public String getNodeId(){
        return nodeId;
    }
}
