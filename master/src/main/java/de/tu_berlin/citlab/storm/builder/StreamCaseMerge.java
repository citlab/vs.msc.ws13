package de.tu_berlin.citlab.storm.builder;

import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.storm.operators.MultipleOperators;
import de.tu_berlin.citlab.storm.operators.OperatorProcessingDescription;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class StreamCaseMerge extends StreamNode {

    protected MultipleOperators multipleOperators;
    protected InputDeclarer declarer;
    protected Fields groupBy;

    public StreamCaseMerge(StreamBuilder builder, MultipleOperators multipleOperators, Fields groupBy ) {
        super(builder);
        this.multipleOperators = multipleOperators;
        this.groupBy = groupBy;
    }

    public StreamCaseMerge case_source(IOperator operator, StreamNode ... sources){
        String[] sourceList = new String[sources.length];
        for( int i=0; i < sources.length; i++ )
            sourceList[i] = sources[i].getNodeId();
        this.multipleOperators.addOperatorProcessingDescription( new OperatorProcessingDescription(operator, sourceList ));
        return this;
    }

    public Fields getGroupByFields(){
        return groupBy;
    }

    public void setDeclarer(InputDeclarer declarer){
        this.declarer = declarer;
    }

    public StreamCaseMergeSource source(StreamNode ... sources){
        StreamCaseMergeSource casemergeSource = new StreamCaseMergeSource(getStreamBuilder(), this, multipleOperators, sources );
        for( StreamNode source : sources ) {
            declarer.fieldsGrouping( source.getNodeId(), getGroupByFields());
        }
        return casemergeSource;
    }

}