package de.tu_berlin.citlab.storm.builder;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.MultipleOperators;
import de.tu_berlin.citlab.storm.operators.OperatorProcessingDescription;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.TupleComparator;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StreamCaseMergeSource extends StreamCaseMerge {

    protected StreamNode[] sources;
    protected StreamCaseMerge casemerge;

    public StreamCaseMergeSource(StreamBuilder builder,StreamCaseMerge casemerge, MultipleOperators multipleOperators, StreamNode ... sources) {
        super(builder,multipleOperators);
        this.sources = sources;
        this.casemerge = casemerge;
    }

    public StreamCaseMerge join( Map<Serializable, List<Tuple>> hashTable,
                            TupleProjection projection,
                            TupleComparator tupleComparator ){

        final StaticHashJoinOperator staticHashJoin =
                new StaticHashJoinOperator(
                        tupleComparator,
                        projection,
                        hashTable );

        String[] sourceList = new String[sources.length];
        for( int i=0; i < sources.length; i++ )
            sourceList[i] = sources[i].getNodeId();

        this.multipleOperators.addOperatorProcessingDescription( new OperatorProcessingDescription( staticHashJoin, sourceList ));

        return casemerge;
    }

    public StreamCaseMerge execute( IOperator operator ) {
        String[] sourceList = new String[sources.length];
        for( int i=0; i < sources.length; i++ )
            sourceList[i] = sources[i].getNodeId();

        this.multipleOperators.addOperatorProcessingDescription( new OperatorProcessingDescription( operator, sourceList ));

        return casemerge;
    }
}
