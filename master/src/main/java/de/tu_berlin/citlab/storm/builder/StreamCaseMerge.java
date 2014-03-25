package de.tu_berlin.citlab.storm.builder;

import de.tu_berlin.citlab.storm.operators.MultipleOperators;
import de.tu_berlin.citlab.storm.operators.OperatorProcessingDescription;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class StreamCaseMerge extends StreamNode {

    private MultipleOperators multipleOperators;

    public StreamCaseMerge(StreamBuilder builder, MultipleOperators multipleOperators ) {
        super(builder);
        this.multipleOperators = multipleOperators;
    }
    public StreamCaseMerge case_source(IOperator operator, StreamNode ... sources){
        String[] sourceList = new String[sources.length];
        for( int i=0; i < sources.length; i++ )
            sourceList[i] = sources[i].getNodeId();
        this.multipleOperators.addOperatorProcessingDescription( new OperatorProcessingDescription(operator, sourceList ));
        return this;
    }

    public StreamCaseMergeSource source(StreamNode ... sources){
        StreamCaseMergeSource casemergeSource = new StreamCaseMergeSource(getStreamBuilder(), this, multipleOperators, sources );
        return casemergeSource;
    }

}