package de.tu_berlin.citlab.storm.builder;


import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.*;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.udf.UDFOutput;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.TupleComparator;

import java.io.Serializable;
import java.util.Iterator;

public class StreamNode implements Serializable {
    public static int StreamNodeCounter = 1;

    protected UDFBolt bolt;
    protected StreamBuilder builder;
    protected String nodeId;

    public StreamNode(StreamBuilder builder){
        StreamNodeCounter++;
        nodeId = this.getClass().getSimpleName()+"-"+StreamNodeCounter;
        this.builder = builder;
    }
    public StreamBuilder getStreamBuilder(){
        return builder;
    }
    public UDFOutput getUDFOutput(){return bolt; }
    public UDFBolt getUDFBolt(){return bolt; }

    public String getNodeId(){
        return nodeId;
    }

    private UDFBolt assignUDF(UDFBolt udf){
        this.bolt = udf;
        return udf;
    }

    public StreamCaseMerge case_merge( Fields groupBy, Fields outputFields ){
        MultipleOperators multiOperators =  new MultipleOperators();

        this.bolt = new UDFBolt(
                outputFields,
                multiOperators,
                getStreamBuilder().getDefaultWindowType(),
                KeyConfigFactory.BySource()
        );
        multiOperators.setUDFBolt(this.bolt);
        StreamCaseMerge case_merge = new StreamCaseMerge(getStreamBuilder(), multiOperators, groupBy );

        InputDeclarer inputDeclarer =
                getStreamBuilder().getTopologyBuilder().setBolt(case_merge.getNodeId(), getUDFBolt() );
        case_merge.setDeclarer(inputDeclarer);

        return case_merge;
    }


    public StreamMerged merge( Fields outputFields, StreamNode ... nodes ){
        StreamMerged merged = new StreamMerged( getStreamBuilder());

        BoltDeclarer boldDeclarer =
        getStreamBuilder().getTopologyBuilder().setBolt(merged.getNodeId(),
                assignUDF(new UDFBolt(
                        outputFields,
                        new IdentityOperator(),
                        getStreamBuilder().getDefaultWindowType(),
                        KeyConfigFactory.BySource()
                ))
        )
        .shuffleGrouping(this.getNodeId());

        for(StreamNode node : nodes ){
            boldDeclarer.shuffleGrouping(node.getNodeId());
        }
        return merged;
    }
    public StreamJoin join( /*Fields groupKey,*/
                            Iterator<Tuple> tuples,
                            TupleProjection projection,
                            TupleComparator comparator,
                            Fields outputFields ){

        StreamJoin node = new StreamJoin( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),

                assignUDF(new UDFBolt(
                        outputFields,
                        new StaticHashJoinOperator(
                                comparator,
                                projection,
                                tuples),
                        getStreamBuilder().getDefaultWindowType(),
                        KeyConfigFactory.BySource()
                ))
        )
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
                assignUDF(new UDFBolt(
                        outputFields,
                        new FilterOperator(filter),
                        getStreamBuilder().getDefaultWindowType()
                ))
        )
        .shuffleGrouping(this.getNodeId());
        
        return node;
    }
    public StreamMapper map( Mapper mapper, Fields outputFields  ){
        StreamMapper node = new StreamMapper( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
                assignUDF(new UDFBolt(
                        outputFields,
                        new MapOperator(mapper),
                        getStreamBuilder().getDefaultWindowType()
                ))
        )
            .shuffleGrouping(this.getNodeId());

        return node;
    }
    public StreamFlatMapper flapMap( FlatMapper flatmapper, Fields outputFields ){
        StreamFlatMapper node = new StreamFlatMapper( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
                assignUDF(new UDFBolt(
                        outputFields,
                        new FlatMapOperator(flatmapper),
                        getStreamBuilder().getDefaultWindowType()
                ))
        )
                .shuffleGrouping(this.getNodeId());
        return node;
    }
    public StreamSink save( de.tu_berlin.citlab.storm.operators.StreamSink streamSink){
        StreamSink node = new StreamSink( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
                assignUDF(new UDFBolt(
                                new Fields(),
                                streamSink,
                                getStreamBuilder().getDefaultWindowType())
                )
        ).shuffleGrouping(this.getNodeId());
        return node;
    }

    public StreamReducer reduce( Fields reduceKey, Reducer reducer, Object init, Fields outputFields ){
        StreamReducer node = new StreamReducer( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt( node.getNodeId(),
                assignUDF(new UDFBolt(
                outputFields,
                new ReduceOperator( reduceKey, reducer, init /*reducer init value */ ),
                getStreamBuilder().getDefaultWindowType(),
                KeyConfigFactory.ByFields( reduceKey )
        ))).fieldsGrouping(this.getNodeId(), reduceKey);
        return node;
    }

    public StreamDelay delay( int sec ){
        StreamDelay node = new StreamDelay( getStreamBuilder());
        getStreamBuilder().getTopologyBuilder().setBolt(node.getNodeId(),
                assignUDF(
                        new UDFBolt(
                                this.getUDFOutput().getOutputFields(),
                                new DelayTuplesOperator(sec),
                                new TimeWindow<Tuple>(sec, sec)
                        )
            )).shuffleGrouping(this.getNodeId());
        return node;
    }
}
